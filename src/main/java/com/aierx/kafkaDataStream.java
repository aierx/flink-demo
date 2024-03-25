package com.aierx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.File;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class kafkaDataStream {
	public static void main(String[] args) throws Exception {
		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("localhost:9092").setTopics("test_aa").setGroupId(
				"data_stream").setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//		DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		
		DataStreamSource<Long> addSource = env.addSource(new TimedSource());
		
		
		 addSource.writeAsText("C:\\Users\\aleiwe\\Desktop\\qr\\flink-demo\\src\\main\\resources\\a.txt");
		addSource.addSink(new PrintSinkFunction<>());
		
		
		env.execute();
	}
}
