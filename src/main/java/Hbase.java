import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.TCPCLientController;
import util.Utils;
import java.io.*;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

class HBase {
    public static void main(String[] args) throws IOException, Exception {
        try {
            Utils utilHbase = new Utils();
            Connection connection = utilHbase.GetConnectionHbase();
            Admin admin = connection.getAdmin();
            utilHbase.CreateTableHbase("MDOTable",connection,utilHbase.getNameCFMDO());
            utilHbase.CreateTableHbase("SYSTable",connection,utilHbase.getNameCFSYS());
            long TTL = 60*60*60*1000;
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
                        utilHbase.insertData(tableMDO,rowName,utilHbase.getNameCFMDO(),utilHbase.getNamecolumMDO(),TTL,rowData);
                        Put p = new Put(Bytes.toBytes(rowName));
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
                        if(typeBegin!=null&&timeStamp!=null&&phoneNumMDO!=null){
                            if (typeBegin.compareToIgnoreCase("Start")==0){
                                dateRowMDO = df.parse(timeStamp);
                                if(dateRowSYS.getTime() >= dateRowMDO.getTime()){
                                    data = data+","+phoneNumMDO;
                                    rowData = data.split(",");
                                    String rowKey= rowData[4]+"_"+rowData[5]+"_"+rowData[1];
                                    //Key Pattern: IPPUBLIC_PortPublic_timestamp
                                    boolean isDone = utilHbase.insertData(tableSYS,rowKey,utilHbase.getNameCFSYS(),utilHbase.getNamecolumSYS(),TTL,rowData);
                                    if (isDone){
                                        System.out.println("Inserted Phone to Table SYS: "+phoneNumMDO);
                                    }
                                }
                            }
                        }
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
