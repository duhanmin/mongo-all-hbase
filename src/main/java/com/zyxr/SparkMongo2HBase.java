package com.zyxr;

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

import com.mongodb.spark.MongoSpark;
import com.mongodb.util.JSON;
import com.zyxr.conf.ConfigurationManager;
import com.zyxr.constant.Constants;

public class SparkMongo2HBase {
	private static final String DB = "mongo_";
	private static final String INFO = "info";
	private static final String _ID = "_id";
	private static final String HOST = ConfigurationManager.getProperty(Constants.MONGO_HOST);
	private static final String HBASE_ZOOKEEPER_QUORUM = ConfigurationManager.getProperty(Constants.ZK_METADATA_BROKER_LIST);
	private static final String MONGO_USER = ConfigurationManager.getProperty(Constants.MONGO_USER);
	private static final String MONGO_PASSWORD = ConfigurationManager.getProperty(Constants.MONGO_PASSWORD);

	public static void main(String[] args) throws IOException {
		if (args.length == 2 || args.length == 1) {
		    try {
				int repartition = Integer.valueOf(args[1]);
				// 可以配置多个需要导入的表，用逗号分割，详见配置文件my.properties/mongo.db.tables
				for (String dbTable : args[0].split(",")) {
					MongoToSparkOnHBase(HOST, dbTable, repartition);
				}
			} catch (Exception e) {
				for (String dbTable : args[0].split(",")) {
					MongoToSparkOnHBase(HOST, dbTable);
				}
			}
		} else {
			System.err.println("参数非法!!!");
			System.err.println("第一个字段必填，如：db.tables -> db.tables1,db.tables2");
			System.err.println("第二个字段可缺省，填入数字即可，推荐填写num-executors乘executor-cores");
			return;
		}
	}

	/**
	 * 
	 * @param repartition
	 * @param host
	 * @param tableName
	 * @throws IOException
	 */
	private static void MongoToSparkOnHBase(String host, String tableName,int repartition ) throws IOException {
		SparkSession spark = SparkSession
				.builder()
				.appName("MongoTosparkOnHBaseTake-" + tableName)
				.master("local[4]")
				.config("spark.mongodb.input.uri","mongodb://" + MONGO_USER + ":" + MONGO_PASSWORD + "@"+ host + ":27017/" + tableName).getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		JavaHBaseContext hbaseContext = getJavaHBaseContext(jsc);

		JavaRDD<String> rdd = MongoSpark.load(jsc).map(f -> f.toJson()).repartition(repartition);

		CommonSetHbase(tableName, rdd, hbaseContext);

		spark.close();
		jsc.close();
	}

	/**
	 * 
	 * @param host
	 * @param tableName
	 * @throws IOException
	 */
	private static void MongoToSparkOnHBase(String host, String tableName)throws IOException {
		SparkSession spark = SparkSession
				.builder()
				.appName("MongoTosparkOnHBaseTake-" + tableName)
				.master("local[4]")
				.config("spark.mongodb.input.uri","mongodb://" + MONGO_USER + ":" + MONGO_PASSWORD + "@"+ host + ":27017/" + tableName).getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		JavaHBaseContext hbaseContext = getJavaHBaseContext(jsc);

		JavaRDD<String> rdd = MongoSpark.load(jsc).map(f -> f.toJson());

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
	private static void CommonSetHbase(String tableName, JavaRDD<String> rdd, JavaHBaseContext hbaseContext) throws IOException {
		String outTableName = DB + tableName;
		CreateHBaseTable(outTableName);
		hbaseContext.bulkPut(rdd, TableName.valueOf(outTableName),new PutFunction());
	}

	/**
	 * 写入HBase接口实现类
	 * 
	 * @author zyxrdu
	 *
	 */
	private static class PutFunction implements Function<String, Put> {
		private static final long serialVersionUID = 1L;

		public Put call(String json) throws Exception {
			@SuppressWarnings("unchecked")
			Map<String, Object> maps = (Map<String, Object>) JSON.parse(json);

			Put put = new Put(Bytes.toBytes(maps.get(_ID).toString()));
			maps.remove(_ID);
			for (Entry<String, Object> entry : maps.entrySet()) {
				// 防止业务不规范存了null
				if (entry.getValue() != null || !entry.getValue().equals(""))
					put.addColumn(Bytes.toBytes(INFO),
							Bytes.toBytes(entry.getKey()),
							Bytes.toBytes(entry.getValue().toString()));
			}
			return put;
		}
	}

	/**
	 * 
	 * @return
	 */
	private static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
		conf.set("hbase.defaults.for.version.skip", "true");
		return conf;
	}

	/**
	 * 获取JavaHBaseContext
	 * 
	 * @param jsc
	 * @return
	 */
	private static JavaHBaseContext getJavaHBaseContext(JavaSparkContext jsc) {
		Configuration conf = getHBaseConfiguration();
		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		return hbaseContext;
	}

	/**
	 * 自动判断表存在与否，如果不存在建表 在建表过程中出现没有命名空间，捕获异常NamespaceNotFoundException，创建命名空间
	 * 
	 * @param tableName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void CreateHBaseTable(String tableName) throws IOException {
		Configuration conf = getHBaseConfiguration();

		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		table.addFamily(new HColumnDescriptor(INFO).setCompressionType(Algorithm.NONE));

		Connection connection = null;
		Admin admin = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e1) {
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
