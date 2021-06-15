package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Utils {
    private String[] nameCFMDO = new String[]{
            "Times",
            "Content",
            "Type",
            "Info",
            "Network"
    };
    private String [][] namecolumMDO = new String[][]{
            {"Timestamp"},
            {"MessageMDO"},
            {"TypeBegin"},
            {"PhoneNumber"},
            {"IPPrivate"},
    };
    private String[] nameCFSYS = new String[]{
            "Site",
            "Times",
            "Network",
            "Info"
    };
    private String [][] namecolumSYS = new String[][]{
            {"SiteName"},
            {"Timestamp"},
            {"IPPrivate","PortPrivate","IPPublic","PortPublic","IPDest","PortDest"},
            {"PhoneNumber"}
    };
    public String[] getNameCFMDO() {
        return nameCFMDO;
    }

    public String[][] getNamecolumMDO() {
        return namecolumMDO;
    }

    public String[] getNameCFSYS() {
        return nameCFSYS;
    }

    public String[][] getNamecolumSYS() {
        return namecolumSYS;
    }

    public Connection GetConnectionHbase() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.clear();
        conf.set("hbase.zookeeper.quorum", "localhost");
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
        if (!admin.tableExists(TableName.valueOf(tableName))){
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String CfName : columFamily){
                tableDescriptor.addFamily(new HColumnDescriptor(CfName));
            }
            admin.createTable(tableDescriptor);
            System.out.println("Table Created: "+tableName);
            return true;
        }
        System.out.println("Fail or Table Existed: "+tableName);
        return false;
    }
    public boolean insertData(Table table,String keyRow,  String []columFamily,String[][] colums,long TTL, String... value){
        Put p = new Put(Bytes.toBytes(keyRow));
        int lenColumFamily = columFamily.length;
        int index = 0;
        for(int i=0;i<lenColumFamily;i++){
            int lenColumeEachFamily = colums[i].length;
            for(int j=0;j<lenColumeEachFamily;j++){
                p.addColumn(Bytes.toBytes(columFamily[i]), Bytes.toBytes(colums[i][j]), Bytes.toBytes(value[index])).setTTL(TTL);
                index++;
            }
        }
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}
