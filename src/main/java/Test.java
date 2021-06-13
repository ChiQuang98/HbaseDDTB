
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
        Configuration conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "192.168.245.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir","/apps/hbase/data");
//            conf.set("zookeeper.znode.parent","/hbase-secure");
        conf.set("hbase.cluster.distributed","false");
        conf.set("zookeeper.znode.parent","/hbase");
//        conf.set("hbase.defaults.for.version.skip", "true");
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        conf.set("hbase.client.retries.number", "2");  // default 35
        conf.set("hbase.rpc.timeout", "10000");  // default 60 secs
        conf.set("hbase.rpc.shortoperation.timeout", "10000"); // default 10 secs

        // Instantiating HBaseAdmin class
//            HBaseAdmin admin = new HBaseAdmin(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        Table hTable = connection.getTable(TableName.valueOf("emp"));
        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes("row1"));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(Bytes.toBytes("personal"),
                Bytes.toBytes("name"),Bytes.toBytes("raju"));

        p.addColumn(Bytes.toBytes("personal"),
                Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

        p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
                Bytes.toBytes("manager"));

        p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
                Bytes.toBytes("50000"));

        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("data inserted");

        // closing HTable
        hTable.close();
    }
}
