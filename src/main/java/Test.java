
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
        Scan scan = new Scan();
        scan.setCaching(5);
//        byte[] rowKeys = Bytes.toBytesBinary("KEY=\\x01\\x01");
//        byte[] fuzzyInfo = {0,0,0,0,1,1};
//        FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(
//                Arrays.asList(
//                        new Pair<byte[], byte[]>(
//                                rowKeys,
//                                fuzzyInfo)));
//        System.out.println("### fuzzyFilter: " + fuzzyFilter.toString());
        scan.addFamily(Bytes.toBytesBinary("Info"));
        scan.setStartRow(Bytes.toBytesBinary("KEY=1"));
//        scan.setStopRow(Bytes.toBytesBinary("KEY=20"));
//        scan.setFilter(fuzzyFilter);
        Utils utilHbase = new Utils();
        Connection connection = utilHbase.GetConnectionHbase();
        Table table = connection.getTable(TableName.valueOf("MDOTable"));
        ResultScanner results = table.getScanner(scan);
        int count = 0;
        int limit = 10;
        for ( Result r : results ) {
            System.out.println("" + r.toString());
            if (count++ >= limit) break;
        }

    }
}
