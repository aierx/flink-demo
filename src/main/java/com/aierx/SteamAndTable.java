package com.aierx;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author leiwenyong
 * @since 2024-03-24
 */
public class SteamAndTable {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
		// enable checkpointing
		Configuration configuration = tableEnv.getConfig().getConfiguration();
		configuration.set(
				ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		configuration.set(
				ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
		
		String sql = "CREATE TABLE sourcedata (`id` bigint,`status` int,`city_id` bigint,`courier_id` bigint,info_index int,order_id bigint,tableName String" +
				") WITH (" +
				"'connector' = 'kafka','topic' = 'canal_monitor_order'," +
				"'properties.bootstrap.servers' = 'bigdata-dev-mq:9092','properties.group.id' = 'testGroup'," +
				"'format' = 'null','scan.startup.mode' = 'earliest-offset')";
		tableEnv.executeSql(sql);
		
	}
}
