package api;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.Utils;

import java.io.IOException;

public class HbaseAPI {
    public void GetRowsByIPSYS(String ipPrivate, int portPrivate, String timeStamp) throws IOException {

//        byte[] rowKeys = Bytes.toBytesBinary("KEY=\\x01\\x01");
//        byte[] fuzzyInfo = {0,0,0,0,1,1};
//        FuzzyRowFilter fuzzyFilter = new FuzzyRowFilter(
//                Arrays.asList(
//                        new Pair<byte[], byte[]>(
//                                rowKeys,
//                                fuzzyInfo)));
//        System.out.println("### fuzzyFilter: " + fuzzyFilter.toString());

        String keyPrefix = ipPrivate+"_"+portPrivate+"_";
        byte[] prefix=Bytes.toBytes(keyPrefix);
        Scan scan = new Scan();
        scan.setRowPrefixFilter(prefix);
        scan.setCaching(5);
        scan.addFamily(Bytes.toBytesBinary("Info"));
        scan.setStartRow(Bytes.toBytesBinary(keyPrefix));
//        scan.setStopRow(Bytes.toBytesBinary("KEY=20"));
//        scan.setFilter(fuzzyFilter);
        Utils utilHbase = new Utils();
        Connection connection = utilHbase.GetConnectionHbase();
        Table table = connection.getTable(TableName.valueOf("SYSTable"));
        ResultScanner results = table.getScanner(scan);
        int count = 0;
        int limit = 10;
        for ( Result r : results ) {
            System.out.println("" + r.toString());
            if (count++ >= limit) break;
        }
    }
}
