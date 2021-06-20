import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import util.Utils;

import java.io.IOException;

public class CreateTable {
    public static void main(String[] args) throws IOException {
//        Utils utilHbase = new Utils();
//        Connection connection = utilHbase.GetConnectionHbase();
//        Admin admin = connection.getAdmin();
//        String []nameCFs = new String[]{
//                "Site",
//                "Times",
//                "Network",
//                "Info"
//        };
//        boolean err = utilHbase.CreateTableHbase("SYSTable",connection,nameCFs);
        Utils utilHbase = new Utils();
        Connection connection = utilHbase.GetConnectionHbase();
        Admin admin = connection.getAdmin();
        String []nameCFs = utilHbase.getNameCFMDO();
        boolean err = utilHbase.CreateTableHbase("MDOTable",connection,nameCFs);
    }
}
