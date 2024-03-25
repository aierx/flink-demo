package com.aierx;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author leiwenyong
 * @since 2024-03-24
 */
public class Table1 {
	
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		
		env.setParallelism(1);
		
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode().build();
		
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		
		tEnv.executeSql(
				"CREATE TABLE simple_test (\n" +
						"  uid BIGINT,\n" +
						"  name STRING,\n" +
						"  category_type INT,\n" +
						"  content BINARY,\n" +
						"  price DOUBLE,\n" +
						"  value_map map<BIGINT, row<v1 BIGINT, v2 INT>>,\n" +
						"  value_arr array<row<v1 BIGINT, v2 INT>>,\n" +
						"  corpus_int INT,\n" +
						"  corpus_str STRING\n" +
						") WITH (\n" +
						" 'connector' = 'kafka',\n" +
						" 'topic' = 'topic',\n" +
						" 'properties.bootstrap.servers' = 'localhost:9092',\n" +
						" 'properties.group.id' = 'testGroup',\n" +
						" 'scan.startup.mode' = 'latest-offset',\n" +
						" 'format' = 'protobuf',\n" +
						" 'protobuf.message-class-name' = 'com.example.SimpleTest',\n" +
						" 'protobuf.ignore-parse-errors' = 'true'\n" +
						")"
		);
		
		Table t = tEnv.sqlQuery("SELECT * FROM simple_test");
		
		tEnv.toAppendStream(t, Row.class).print();
		
		env.execute();
		
	}
}
