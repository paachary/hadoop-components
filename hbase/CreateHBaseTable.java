import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateHBaseTable {

    public static void main(String[] args) throws IOException {

        byte[] rating = Bytes.toBytes("rating");
        Configuration conf =  HBaseConfiguration.create();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.addResource("/usr/hdp/current/hbase-master/conf/hbase-site.xml");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ratings");

        TableDescriptor tableDescriptor = TableDescriptorBuilder.
                newBuilder(tableName).
                setColumnFamily(ColumnFamilyDescriptorBuilder.of(rating)).build();

        admin.createTable(tableDescriptor);
        Table table = conn.getTable(tableName);

        List<Put> puts = new ArrayList<Put>(4);

        byte[] userid = Bytes.toBytes(1);
        byte[] movieid = Bytes.toBytes(1);
        byte[] ratingVal = Bytes.toBytes(4.5);
        Put put = new Put(userid);
        put.addColumn(rating,movieid,rating);
        puts.add(put);
        userid = Bytes.toBytes(1);
        movieid = Bytes.toBytes(2);
        ratingVal = Bytes.toBytes(4.5);
        put = new Put(userid);
        put.addColumn(rating,movieid,rating);
        puts.add(put);
        userid = Bytes.toBytes(1);
        movieid = Bytes.toBytes(4);
        ratingVal = Bytes.toBytes(4);
        put = new Put(userid);
        put.addColumn(rating,movieid,rating);
        puts.add(put);
        userid = Bytes.toBytes(4);
        movieid = Bytes.toBytes(10);
        ratingVal = Bytes.toBytes(1);
        put = new Put(userid);
        put.addColumn(rating,movieid,rating);
        puts.add(put);

        try  {
            table.put(puts);
            System.out.println("inserted values in the table");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Get get = new Get(Bytes.toBytes(1));
        get.addColumn(rating, movieid);
        Result result = table.get(get);
        byte[] val = result.getValue(rating,
                movieid);
        System.out.println("Value: " + Bytes.toString(val));
        System.out.println("table:" + tableName);
        }
}
