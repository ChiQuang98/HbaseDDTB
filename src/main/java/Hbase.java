import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.TCPCLientController;
import util.Utils;


import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;

class HBase{
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
    public static void main(String[] args) throws IOException, Exception{
        try {
            Utils utilHbase = new Utils();
            Connection connection = utilHbase.GetConnectionHbase();
            Admin admin = connection.getAdmin();
            String []nameCFs = new String[]{
                    "Times",
                    "Content",
                    "Type",
                    "Info",
                    "Network"
            };

//            boolean err = utilHbase.CreateTableHbase("MDOTable",connection,nameCFs);
            final TCPCLientController clientController1 = new TCPCLientController(InetAddress.getByName("10.4.200.61"), 11000);
            final TCPCLientController clientController2 = new TCPCLientController(InetAddress.getByName("10.4.200.61"), 11001);
            //MDO Data Streaming
            new Thread(() -> {
                try {
                    long   index = 0;
                    UUID uuid = UUID.randomUUID();
                    Table tableMDO = connection.getTable(TableName.valueOf("MDOTable"));
                    while (true) {
                        String data = clientController1.readData();
                        // add to queue
                        queue.add(data);
//                        System.out.println(data);
                        String [] rowData = data.split("\\|");
                        //Tim xem IPPrivate da co trong co so du lieu chua
                        //Neu co thi co nhieu row
//                        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("Network"),
//                                Bytes.toBytes("IPPrivate"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(rowData[4]));
//                        Scan scan = new Scan();
////
//                        scan.addFamily(Bytes.toBytes("Times"));
//                        scan.addColumn(Bytes.toBytes("Content"), Bytes.toBytes("da"));
//                        scan.addColumn(Bytes.toBytes("Type"), Bytes.toBytes("gross"));
//                        scan.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("gross"));
//                        scan.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("gross"));
//                        scan.setFilter(filter);
//                        ResultScanner rs = tableMDO.getScanner(scan);


//                        String rowName = uuid.toString();
                        String rowName = "row"+index;
                        Put p = new Put(Bytes.toBytes(rowName));
                        p.addColumn(Bytes.toBytes(nameCFs[0]),Bytes.toBytes("Timestamp"),Bytes.toBytes(rowData[0])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFs[1]),Bytes.toBytes("MessageMDO"),Bytes.toBytes(rowData[1])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFs[2]),Bytes.toBytes("TypeBegin"),Bytes.toBytes(rowData[2])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFs[3]),Bytes.toBytes("PhoneNumber"),Bytes.toBytes(rowData[3])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFs[4]),Bytes.toBytes("IPPrivate"),Bytes.toBytes(rowData[4])).setTTL(60*1000);
                        tableMDO.put(p);
                        index++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            // read data from server 2
            new Thread(() -> {
                try {
                    long   index = 0;
                    UUID uuid = UUID.randomUUID();
                    String []nameCFSYS = new String[]{
                            "Site",
                            "Times",
                            "Network",
                            "Info"
                    };
                    Table tableSYS = connection.getTable(TableName.valueOf("SYSTable"));
                    while (true) {
                        String data = clientController2.readData();
                        // add to queue
                        queue.add(data);
                        System.out.println(data);
                        String [] rowData = data.split(",");
                        String rowName = "row"+index;
                        Put p = new Put(Bytes.toBytes(rowName));
                        p.addColumn(Bytes.toBytes(nameCFSYS[0]),Bytes.toBytes("SiteName"),Bytes.toBytes(rowData[0])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[1]),Bytes.toBytes("Timestamp"),Bytes.toBytes(rowData[1])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("IPPrivate"),Bytes.toBytes(rowData[2])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("PortPrivate"),Bytes.toBytes(rowData[3])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("IPPublic"),Bytes.toBytes(rowData[4])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("PortPublic"),Bytes.toBytes(rowData[5])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("IPDest"),Bytes.toBytes(rowData[6])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[2]),Bytes.toBytes("PortDest"),Bytes.toBytes(rowData[7])).setTTL(60*1000);
                        p.addColumn(Bytes.toBytes(nameCFSYS[3]),Bytes.toBytes("PhoneNumber"),Bytes.toBytes("TempNumber:0975312798")).setTTL(60*1000);
                        tableSYS.put(p);
                        index++;

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (Exception exp) {
            System.out.println("fail");
            System.out.println( ""+exp.getMessage());
        }


    }

}
