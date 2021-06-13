import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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
            final TCPCLientController clientController1 = new TCPCLientController(InetAddress.getByName("localhost"), 11000);
//            final TCPCLientController clientController2 = new TCPCLientController(InetAddress.getByName("localhost"), 11001);
            //MDO Data Streaming
            new Thread(() -> {
                try {
                    UUID uuid = UUID.randomUUID();
                    Table tableMDO = connection.getTable(TableName.valueOf("MDO"));
                    while (true) {
                        String data = clientController1.readData();
                        // add to queue
                        queue.add(data);
                        System.out.println(data);
                        String [] rowData = data.split("\\|");
                        String rowName = uuid.toString();
                        Put p = new Put(Bytes.toBytes(rowName));
                        p.addColumn(Bytes.toBytes(nameCFs[0]),Bytes.toBytes("Timestamp"),Bytes.toBytes(rowData[0]));
                        p.addColumn(Bytes.toBytes(nameCFs[1]),Bytes.toBytes("MessageMDO"),Bytes.toBytes(rowData[1]));
                        p.addColumn(Bytes.toBytes(nameCFs[2]),Bytes.toBytes("TypeBegin"),Bytes.toBytes(rowData[2]));
                        p.addColumn(Bytes.toBytes(nameCFs[3]),Bytes.toBytes("PhoneNumber"),Bytes.toBytes(rowData[3]));
                        p.addColumn(Bytes.toBytes(nameCFs[4]),Bytes.toBytes("IPPrivate"),Bytes.toBytes(rowData[4]));
                        tableMDO.put(p);
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
