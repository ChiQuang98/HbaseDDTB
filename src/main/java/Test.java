
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import util.Utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {

        PrintWriter writer = new PrintWriter(new FileOutputStream("SYSTest.txt",true));
        Utils utilHbase = new Utils();
        Connection connection = utilHbase.GetConnectionHbase();
//        while (true) {
//            String data = clientController2.readData();
             writer.println("data");
            int count = 0;

            Table tableSYS = connection.getTable(TableName.valueOf("MDOTable"));
            Get get = new Get(Bytes.toBytes("KEY_" +"182.8.138.214"));
            get.addFamily(Bytes.toBytes("Info"));
            get.addFamily(Bytes.toBytes("Times"));
            get.addFamily(Bytes.toBytes("Type"));
            Result result = tableSYS.get(get);
            String phoneNumMDO = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("PhoneNumber")));
            String timestamp = Bytes.toString(result.getValue(Bytes.toBytes("Times"), Bytes.toBytes("Timestamp")));
            String typeBegin = Bytes.toString(result.getValue(Bytes.toBytes("Type"), Bytes.toBytes("TypeBegin")));
            System.out.println(phoneNumMDO);


    }
}
