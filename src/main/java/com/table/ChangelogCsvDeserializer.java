package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {
	
	private final List<LogicalType> parsingTypes;
	private final DynamicTableSource.DataStructureConverter converter;
	private final TypeInformation<RowData> producedTypeInfo;
	private final String columnDelimiter;
	
	public ChangelogCsvDeserializer(
			List<LogicalType> parsingTypes,
			DynamicTableSource.DataStructureConverter converter,
			TypeInformation<RowData> producedTypeInfo,
			String columnDelimiter) {
		this.parsingTypes = parsingTypes;
		this.converter = converter;
		this.producedTypeInfo = producedTypeInfo;
		this.columnDelimiter = columnDelimiter;
	}
	
	@Override
	public RowData deserialize(byte[] message) throws IOException {
		// 按列解析数据，其中一列是 changelog 标记
		final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
		final RowKind kind = RowKind.valueOf(columns[0]);
		final Row row = new Row(kind, parsingTypes.size());
		for (int i = 0; i < parsingTypes.size(); i++) {
			row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
		}
		// 转换为内部数据结构
		return (RowData) converter.toInternal(row);
	}
	
	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}
	
	private static Object parse(LogicalTypeRoot root, String value) {
		switch (root) {
			case INTEGER:
				return Integer.parseInt(value);
			case VARCHAR:
				return value;
			default:
				throw new IllegalArgumentException();
		}
	}
	
	@Override
	public void open(InitializationContext context) throws Exception {
		// 转化器必须要被开启。
		converter.open(RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
	}
	
	@Override
	public TypeInformation<RowData> getProducedType() {
		// 为 Flink 的核心接口提供类型信息。
		return producedTypeInfo;
	}
	

}
