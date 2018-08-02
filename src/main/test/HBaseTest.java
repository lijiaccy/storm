/*
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

    public static Configuration getHBaseConfiguration() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // zookeeper的配置信息
        conf.set("hbase.zookeeper.quorum","172.30.154.110");// zookeeper节点信息
        conf.set("hbase.zookeeper.property.clientPort","2181");// zookeeper端口
        // conf.set("dfs.socket.timeout", "180000");
        return conf;
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseTest.getHBaseConfiguration();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        try {
//            createTestTable(hBaseAdmin);
//            deleteTestTable(hBaseAdmin);
            describTestTable(hBaseAdmin);
        } finally {
            hBaseAdmin.close(); // 资源释放
        }
    }

    static void createTestTable(HBaseAdmin hbAdmin) throws IOException {
        TableName tableName = TableName.valueOf("lijia"); // 创建表名
        HTableDescriptor hDescriptor = new HTableDescriptor(tableName);
        hDescriptor.addFamily(new HColumnDescriptor("c1"));// 给定列族
        hbAdmin.createTable(hDescriptor);
        System.out.println("创建表成功!");
    }
    static void deleteTestTable(HBaseAdmin hbAdmin) throws IOException {
        String tablename =  "lijia" ;
        if (hbAdmin.tableExists(tablename)) {
            try  {
                hbAdmin.disableTable(tablename);
                hbAdmin.deleteTable(tablename);
            }  catch  (Exception e) {
                //  TODO : handle exception
                e.printStackTrace();
            }
        }
        hbAdmin.close();
        System.out.println("删除表成功");
    }

    static void listTestTable(HBaseAdmin hBaseAdmin) throws Exception{
        TableName[] tableNames = hBaseAdmin.listTableNames();
        for (TableName t: tableNames) {
            System.out.println(t.getNameAsString());
        }
    }
    static void columnTestTable(HBaseAdmin hBaseAdmin) throws Exception{
        hBaseAdmin.addColumn("lijia",new HColumnDescriptor("c4"));
        hBaseAdmin.deleteColumn("lijia","c4");
    }

    static void describTestTable(HBaseAdmin hBaseAdmin) throws Exception{
        HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor("lijia".getBytes());
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor hc: columnFamilies ) {
            System.out.println(hc.getNameAsString());

        }
    }

    static void putTestTable(HBaseAdmin hBaseAdmin) throws Exception{
        Configuration configuration = HBaseTest.getHBaseConfiguration();
        Put put = new Put("lijia".getBytes());
        put.add("cf1".getBytes(), "colum1".getBytes(), "value1".getBytes()) ;
        put.add("cf1".getBytes(), "colum2".getBytes(), "value2".getBytes()) ;
        put.add("cf1".getBytes(), "colum3".getBytes(), "value3".getBytes()) ;
        Put put2 = new Put("234".getBytes()) ;
        put2.add("cf1".getBytes(), "colum1".getBytes(), "value1".getBytes()) ;
        put2.add("cf1".getBytes(), "colum2".getBytes(), "value2".getBytes()) ;
        put2.add("cf1".getBytes(), "colum3".getBytes(), "value3".getBytes()) ;

        HTable table = new HTable(configuration, "lijia");
        table.put(put);
        table.put(put2);
    }

    public static void deleteRow(String tableName,String rowKey) throws Exception{
        Configuration configuration = HBaseTest.getHBaseConfiguration();
        HTable hTable = new HTable(configuration,tableName);
        Delete delete = new Delete(rowKey.getBytes());
        List<Delete> list = new ArrayList<Delete>();
        list.add(delete);
        hTable.delete(list);
    }




}*/
