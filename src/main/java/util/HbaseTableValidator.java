package util;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTableValidator {
    private static final Get DUMMY_GET = new Get(Bytes.toBytes(0));

    public static boolean tableExists (Table table) throws IOException {
        // ONLY WAY TO TELL WHETHER HBASE TABLE EXISTS IS TO TRY TO ACCESS IT!!
        try {
            table.get(DUMMY_GET);
        } catch (TableNotFoundException e) {
            return false;
        }
        return true;
    }
}
