package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class SocketDynamicTableSource implements ScanTableSource {
	
	
	private final String hostname;
	private final int port;
	private final byte byteDelimiter;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType producedDataType;
	
	public SocketDynamicTableSource(
			String hostname,
			int port,
			byte byteDelimiter,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType producedDataType) {
		this.hostname = hostname;
		this.port = port;
		this.byteDelimiter = byteDelimiter;
		this.decodingFormat = decodingFormat;
		this.producedDataType = producedDataType;
	}
	
	@Override
	public ChangelogMode getChangelogMode() {
		// 在我们的例子中，由解码器来决定 changelog 支持的模式
		// 但是在 source 端指定也可以
		return decodingFormat.getChangelogMode();
	}
	
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		// 创建运行时类用于提交给集群
		
		final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
				runtimeProviderContext,
				producedDataType);
		
		final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(
				hostname,
				port,
				byteDelimiter,
				deserializer);
		
		return SourceFunctionProvider.of(sourceFunction, false);
	}
	
	@Override
	public DynamicTableSource copy() {
		return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
	}
	
	@Override
	public String asSummaryString() {
		return "Socket Table Source";
	}
}
