package com.aierx;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class TestCustomTableApi {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
		
		EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
		
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);
		
		
		
		tEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)\n" +
				"WITH (\n" +
				"  'connector' = 'socket',\n" +
				"  'hostname' = '192.168.1.3',\n" +
				"  'port' = '9999',\n" +
				"  'byte-delimiter' = '10',\n" +
				"  'format' = 'changelog-csv',\n" +
				"  'changelog-csv.column-delimiter' = '|'\n" +
				");");
		
		
		Table table = tEnv.sqlQuery("select * from UserScores");
		
		DataStream<Row> changelogStream = tEnv.toChangelogStream(table);
		
		changelogStream.addSink(new PrintSinkFunction<>());
		
		changelogStream.writeAsText("C:\\Users\\aleiwe\\Desktop\\qr\\flink-demo\\src\\main\\java\\com" +
				"\\table\\a", FileSystem.WriteMode.OVERWRITE);
		
		env.execute();
		
	}
}
