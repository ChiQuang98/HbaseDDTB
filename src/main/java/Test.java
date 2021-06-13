
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.Utils;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        String s = "NVL01_1,20210614030043,241.66.85.130,3441,76.29.202.87,6997,82.94.184.253,7704";
        String [] ar = s.split(",");
        System.out.println(ar[0]);
        System.out.println(ar.length);
    }
}
