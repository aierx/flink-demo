package com.aierx;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.Set;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class PbFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
				return null;
			}
			
			@Override
			public ChangelogMode getChangelogMode() {
				return null;
			}
		};
	}
	
	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
		return new EncodingFormat<SerializationSchema<RowData>>() {
			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType dataType) {
				return null;
			}
			
			@Override
			public ChangelogMode getChangelogMode() {
				return null;
			}
		};
	}
	
	@Override
	public String factoryIdentifier() {
		return "proto";
	}
	
	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}
	
	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Collections.emptySet();
	}
}
