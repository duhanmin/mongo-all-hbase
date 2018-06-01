package com.du;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;


import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.util.JSON;

public class SparkMongo2HBase {
	public static String DB = "mongo_";
//	public static String HOST = "192.168.9.71";
	public static String HOST = "192.168.9.145";
	public static void main(String[] args) throws IOException {
		
		MongoClient mongo = new MongoClient(HOST, 27017);  
		
//        //查询所有的库
//        for (String database : mongo.listDatabaseNames()) {  
//        	
//            MongoDatabase db = mongo.getDatabase(database);  
//            
//            //查询所有的表
//            for (String tableName : db.listCollectionNames()) {  
//            	
//            	MongoToSparkOnHBase(HOST,database + "." + tableName);
//            	
//            }  
//        }  
		  MongoToSparkOnHBase(HOST,"109db.c_user_address_book");
		
        mongo.close();	
	}
	
	/**
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void MongoToSparkOnHBase(String host,String tableName) throws IOException{
		
			SparkSession spark = SparkSession
					.builder()
					.appName("MongoTosparkOnHBaseTake-" + tableName)
					.master("local[4]")
					.config("spark.sql.warehouse.dir","file:////C://spark-warehouse")
//					.config("spark.mongodb.input.uri", "mongodb://"+host+ "/" + tableName)
					.config("spark.mongodb.input.uri", "mongodb://dba109:dba109@"+host+ ":27017/" + tableName)
					.getOrCreate();
			
			JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
			
			JavaHBaseContext hbaseContext = getJavaHBaseContext(jsc);
		
	 		JavaRDD<String> rdd = MongoSpark.load(jsc).map(f -> f.toJson()).repartition(10);
	
	 		CommonSetHbase(tableName, rdd, hbaseContext);
	 		
			spark.close();
			jsc.close();
	}
	
	/**
	 * 
	 * @param tableName
	 * @param rdd
	 * @param hbaseContext
	 * @throws IOException
	 */
	public static void CommonSetHbase(String tableName,JavaRDD<String> rdd,JavaHBaseContext hbaseContext) throws IOException{
		String outTableName = DB + tableName;
		CreateHBaseTable(outTableName);
		hbaseContext.bulkPut(rdd, TableName.valueOf(outTableName),new PutFunction());
	}
	/**
	 * 写入HBase接口实现类
	 * @author zyxrdu
	 *
	 */
	public static class PutFunction implements Function<String, Put> {
		
		private static final long serialVersionUID = 1L;
		
		public Put call(String json) throws Exception {
			@SuppressWarnings("unchecked")
			Map<String, Object> maps = (Map<String, Object>) JSON.parse(json);
			
			Put put = new Put(Bytes.toBytes(maps.get("_id").toString()));
			
			for (Entry<String, Object> entry : maps.entrySet()) {
				//防止业务不规范存了null
				if(entry.getValue() != null || !entry.getValue().equals(""))
					put.addColumn(Bytes.toBytes("info"), 
						Bytes.toBytes(entry.getKey()),
						Bytes.toBytes(entry.getValue().toString()));
			}
			return put;
		}
	}
	
	public static Configuration getHBaseConfiguration(){
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "node71:2181,node72:2181,node73:2181");
		conf.set("hbase.defaults.for.version.skip", "true");

        return conf;
    }
	
	/**
	 * 获取JavaHBaseContext
	 * @param jsc
	 * @return
	 */
	public static JavaHBaseContext getJavaHBaseContext(JavaSparkContext jsc){
		
		Configuration conf = getHBaseConfiguration();
		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		
        return hbaseContext;
    }
	
	/**
	 * 自动判断表存在与否，如果不存在建表
	 * 在建表过程中出现没有命名空间，捕获异常NamespaceNotFoundException，创建命名空间
	 * @param tableName
	 * @throws IOException
	 * @throws InterruptedException 
	 */
    @SuppressWarnings("null")
	public static void CreateHBaseTable(String tableName ) throws IOException{
        
    	Configuration conf = getHBaseConfiguration();
		
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        table.addFamily(new HColumnDescriptor("info").setCompressionType(Algorithm.NONE));
        
        Connection connection = null;
        Admin admin = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e1) {
			admin.close();
			connection.close();
		}
        
        TableName tName = table.getTableName();
        
		if (!admin.tableExists(tName)) {
			try {
				admin.createTable(table);
			} catch (IOException e) {
				admin.createNamespace(NamespaceDescriptor.create(tName.getNameAsString().split(":")[0]).build());
				admin.createTable(table);
			}
		}
		admin.close();
		connection.close();
    }
}
