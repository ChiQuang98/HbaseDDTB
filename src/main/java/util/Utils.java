package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class Utils {
    public Connection GetConnectionHbase() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "192.168.245.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir","/apps/hbase/data");
//            conf.set("zookeeper.znode.parent","/hbase-secure");
        conf.set("hbase.cluster.distributed","false");
        conf.set("zookeeper.znode.parent","/hbase");
//        conf.set("hbase.defaults.for.version.skip", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        conf.set("hbase.client.retries.number", "2");  // default 35
        conf.set("hbase.rpc.timeout", "10000");  // default 60 secs
        conf.set("hbase.rpc.shortoperation.timeout", "10000"); // default 10 secs

        // Instantiating HBaseAdmin class
//            HBaseAdmin admin = new HBaseAdmin(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        if (connection!=null){
            return connection;
        }
        return null;
    }
    public boolean CreateTableHbase(String tableName, Connection connection, String... columFamily) throws IOException {
//        Connection connection;
        Admin admin = connection.getAdmin();
        int numCF = columFamily.length;
//        if (!admin.tableExists(TableName.valueOf(tableName))){
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String CfName : columFamily){
                tableDescriptor.addFamily(new HColumnDescriptor(CfName));
            }
            admin.createTable(tableDescriptor);
            System.out.println("Table Created");
            return true;
//        }
//        System.out.println("Fail Create Table, Table Existed");
//        return false;
    }
}
