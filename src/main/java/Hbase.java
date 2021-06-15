import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import sun.util.calendar.BaseCalendar;
import util.TCPCLientController;
import util.Utils;


import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

class HBase {
    public static PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>(1000000, (s1, s2) -> {
        // comparator so that element go in the queue will be sorted
        for (int i = 0; i < 14; i++) {
            if (s1.charAt(i) < s2.charAt(i + 8)) return -1;
            else if (s1.charAt(i) > s2.charAt(i)) return 1;
        }
        if (s1.startsWith("NVL01")) return 1;
        else if (s2.startsWith("NVL01")) return -1;
        return 0;
    });

    public static void main(String[] args) throws IOException, Exception {
        try {
            Utils utilHbase = new Utils();
            Connection connection = utilHbase.GetConnectionHbase();
            Admin admin = connection.getAdmin();
            String[] nameCFs = new String[]{
                    "Times",
                    "Content",
                    "Type",
                    "Info",
                    "Network"
            };
            long TTL = 60*60*60 * 1000;
//            boolean err = utilHbase.CreateTableHbase("MDOTable",connection,nameCFs);
            final TCPCLientController clientController1 = new TCPCLientController(InetAddress.getByName("10.4.200.61"), 11000);
            final TCPCLientController clientController2 = new TCPCLientController(InetAddress.getByName("10.4.200.61"), 11001);
            //MDO Data Streaming
            new Thread(() -> {
                try {
                    PrintWriter writer = new PrintWriter("MDO.txt", "UTF-8");
                    long index = 0;
                    UUID uuid = UUID.randomUUID();
                    Table tableMDO = connection.getTable(TableName.valueOf("MDOTable"));
                    while (true) {
                        String data = clientController1.readData();
                        writer.println(data);
                        // add to queue
//                        queue.add(data);
//                        System.out.println(data);
                        String[] rowData = data.split("\\|");
                        String rowName = "KEY_" + rowData[4];
                        // KEY_IPPRIVATE
                        Put p = new Put(Bytes.toBytes(rowName));
                        p.addColumn(Bytes.toBytes(nameCFs[0]), Bytes.toBytes("Timestamp"), Bytes.toBytes(rowData[0])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFs[1]), Bytes.toBytes("MessageMDO"), Bytes.toBytes(rowData[1])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFs[2]), Bytes.toBytes("TypeBegin"), Bytes.toBytes(rowData[2])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFs[3]), Bytes.toBytes("PhoneNumber"), Bytes.toBytes(rowData[3])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFs[4]), Bytes.toBytes("IPPrivate"), Bytes.toBytes(rowData[4])).setTTL(TTL);
                        tableMDO.put(p);
                        index++;
                    }
                } catch (Exception e) {
                    System.exit(1);
                    e.printStackTrace();
                }
            }).start();
            new Thread(() -> {
                try {
                    PrintWriter writer = new PrintWriter("SYS.txt", "UTF-8");
//                    AtomicReference<PrintWriter> writer = new AtomicReference<>(new PrintWriter(new FileOutputStream("SYS.txt", true)));
                    long index = 0;
                    UUID uuid = UUID.randomUUID();
                    String[] nameCFSYS = new String[]{
                            "Site",
                            "Times",
                            "Network",
                            "Info"
                    };
                    Table tableSYS = connection.getTable(TableName.valueOf("SYSTable"));
                    Table tableMDO = connection.getTable(TableName.valueOf("MDOTable"));
                    Scan scan = new Scan();
                    while (true) {
                        String data = clientController2.readData();
//                        writer.get().println(data);
                        writer.flush();
                        writer.println(data);
                        String[] rowData = data.split(",");
                        String rowName = "row" + index;
//                        System.out.println(rowName);
                        Put p = new Put(Bytes.toBytes(rowName));
                        p.addColumn(Bytes.toBytes(nameCFSYS[0]), Bytes.toBytes("SiteName"), Bytes.toBytes(rowData[0])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[1]), Bytes.toBytes("Timestamp"), Bytes.toBytes(rowData[1])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("IPPrivate"), Bytes.toBytes(rowData[2])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("PortPrivate"), Bytes.toBytes(rowData[3])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("IPPublic"), Bytes.toBytes(rowData[4])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("PortPublic"), Bytes.toBytes(rowData[5])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("IPDest"), Bytes.toBytes(rowData[6])).setTTL(TTL);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]), Bytes.toBytes("PortDest"), Bytes.toBytes(rowData[7])).setTTL(TTL);
//                        p.addColumn(Bytes.toBytes(nameCFSYS[3]), Bytes.toBytes("PhoneNumber"), Bytes.toBytes("PhoneNumberMDO")).setTTL(TTL);
                        Date dateRowSYS, dateRowMDO;
                        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
                        dateRowSYS = df.parse(rowData[1]);

                        Get get = new Get(Bytes.toBytes("KEY_" + rowData[2]));
                        get.addFamily(Bytes.toBytes("Info"));
                        get.addFamily(Bytes.toBytes("Times"));
                        get.addFamily(Bytes.toBytes("Type"));
                        Result result = tableMDO.get(get);
                        String phoneNumMDO = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("PhoneNumber")));
                        String timeStamp = Bytes.toString(result.getValue(Bytes.toBytes("Times"), Bytes.toBytes("Timestamp")));
                        String typeBegin = Bytes.toString(result.getValue(Bytes.toBytes("Type"), Bytes.toBytes("TypeBegin")));
                        System.out.println(phoneNumMDO);
                        if(typeBegin!=null&&timeStamp!=null&&phoneNumMDO!=null){
                            if (typeBegin.compareToIgnoreCase("Start")==0){
                                dateRowMDO = df.parse(timeStamp);
                                if(dateRowSYS.getTime() >= dateRowMDO.getTime()){
                                    p.addColumn(Bytes.toBytes(nameCFSYS[3]), Bytes.toBytes("PhoneNumber"), Bytes.toBytes(phoneNumMDO)).setTTL(TTL);
                                    tableSYS.put(p);
                                }
                            }
                        }
                        index++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (Exception exp) {
            System.out.println("fail");
            System.out.println("" + exp.getMessage());
        }


    }


}
