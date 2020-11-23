import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
public class main {
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum","172.26.41.189");
    }

    public static void main(String[] args) throws IOException {
       createTable("students");
        insertData("students", "001", "ID", "ID", "001");
        insertData("students", "002", "ID", "ID", "002");
        insertData("students", "003", "ID", "ID", "003");
        //
        insertData("students", "001", "Description", "Name", "Li Lei");
        insertData("students", "001", "Description", "Height", "176");
        insertData("students", "001", "Courses", "Chinese", "80");
        insertData("students", "001", "Courses", "Math", "90");
        insertData("students", "001", "Courses", "Physics", "95");
        insertData("students", "001", "Home", "Province", "Zhejiang");
        //
        insertData("students", "002", "Description", "Name", "Han Meimei");
        insertData("students", "002", "Description", "Height", "183");
        insertData("students", "002", "Courses", "Chinese", "88");
        insertData("students", "002", "Courses", "Math", "77");
        insertData("students", "002", "Courses", "Physics", "66");
        insertData("students", "002", "Home", "Province", "Beijing");
        //
        insertData("students", "003", "Description", "Name", "Xiao Ming");
        insertData("students", "003", "Description", "Height", "162");
        insertData("students", "003", "Courses", "Chinese", "90");
        insertData("students", "003", "Courses", "Math", "90");
        insertData("students", "003", "Courses", "Physics", "90");
        insertData("students", "003", "Home", "Province", "Shanghai");
        //扫描创建后的students表
        ScanAll("students");
        //查询学生来自的省；
        QueryHome("students");
        //增加新的列Courses:English，并添加数据
        insertData("students", "001", "Courses", "English", "99");
        //增加新的列族Contact和新的Contact:Email，并添加数据；
        AddColoumn("students","Contact");
        insertData("students", "001", "Contact", "Email", "super_handsome_wsq@truth.com");
        //删除students
        dropTable("students");

    }


    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist,detele....");
            }
            HTableDescriptor t = new HTableDescriptor(tableName);
            t.addFamily(new HColumnDescriptor("ID"));
            t.addFamily(new HColumnDescriptor("Description"));
            t.addFamily(new HColumnDescriptor("Courses"));
            t.addFamily(new HColumnDescriptor("Home"));
            hBaseAdmin.createTable(t);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }


    public static void insertData(String tableName, String rowKey, String family, String
            qualifier, String value) throws IOException {
        HTable table = new HTable(conf, tableName);
        System.out.println("start insert data ......");
        Put put = new Put(rowKey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");
    }


    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void ScanAll(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())+"/"+ new String(keyValue.getQualifier())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void  AddColoumn(String table,String family) throws IOException{

            // Instantiating configuration class.
            Configuration conf = HBaseConfiguration.create();
            // Instantiating HBaseAdmin class.
            HBaseAdmin admin = new HBaseAdmin(conf);
            // Instantiating columnDescriptor class
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            // Adding column family
            admin.addColumn(table, columnDescriptor);
            System.out.println("coloumn added");
    }

    public static void QueryHome(String tableName) throws IOException {

        HTable table = new HTable(conf, tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                String name = "";
                for (KeyValue keyValue : r.raw()) {
                    String family = new String(keyValue.getFamily());
                    String qualifier = new String(keyValue.getQualifier());
                    String value = new String(keyValue.getValue());
                    if (family.equals("Description") && qualifier.equals("Name")) {
                        name = new String(keyValue.getValue());
                    }
                    if (family.equals( "Home") && qualifier.equals( "Province")) {
                        System.out.println("名字：" + name
                                + "====Home:" + value);
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
