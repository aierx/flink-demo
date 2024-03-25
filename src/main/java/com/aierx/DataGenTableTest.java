package com.aierx;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class DataGenTableTest {
	
	
	public static void main(String[] args) throws Exception {
		
		
		EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		
		
		
		tEnv.executeSql("CREATE TABLE Orders (\n" +
				"    order_number BIGINT,\n" +
				"    price        DECIMAL(32,2),\n" +
				"    buyer        ROW<first_name STRING, last_name STRING>,\n" +
				"    order_time   TIMESTAMP(3)\n" +
				") WITH (\n" +
				"  'connector' = 'datagen',\n" +
				"  'number-of-rows' = '10',\n" +
				"  'format' = 'proto'\n" +
				")");
		
		
		Table table = tEnv.sqlQuery("select * from Orders;");
		
		
		DataStream<Row> dataStream = tEnv.toChangelogStream(table);
		
		
		dataStream.print();
		
		
		env.execute("aaa");
		
		
	}
}
