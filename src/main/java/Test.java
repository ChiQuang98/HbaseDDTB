
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
        byte[] rowKeys = Bytes.toBytesBinary("???.01");
        byte[] fuzzyInfo = {0x01,0x01,0x01,0x01,0x00,0x00};
        FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(
                Arrays.asList(
                        new Pair<byte[], byte[]>(
                                rowKeys,
                                fuzzyInfo)));
        System.out.println("### fuzzyFilter: " + fuzzyFilter.toString());
        scan.addFamily(Bytes.toBytesBinary("colfam1"));
        scan.setStartRow(Bytes.toBytesBinary("row-01"));
        scan.setStopRow(Bytes.toBytesBinary("row-05"));
        scan.setFilter(fuzzyFilter);


    }
}
