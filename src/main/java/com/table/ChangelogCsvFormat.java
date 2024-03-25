package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {
	
	
	private final String columnDelimiter;
	
	public ChangelogCsvFormat(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}
	@Override
	@SuppressWarnings("unchecked")
	public DeserializationSchema<RowData> createRuntimeDecoder(
			DynamicTableSource.Context context,
			DataType producedDataType) {
		// 为 DeserializationSchema 创建类型信息
		final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(
				producedDataType);
		
		// DeserializationSchema 中的大多数代码无法处理内部数据结构
		// 在最后为转换创建一个转换器
		final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);
		
		// 在运行时，为解析过程提供逻辑类型
		final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();
		
		// 创建运行时类
		return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
	}
	
	@Override
	public ChangelogMode getChangelogMode() {
		// 支持处理 `INSERT`、`DELETE` 变更类型的数据。
		return ChangelogMode.newBuilder()
				.addContainedKind(RowKind.INSERT)
				.addContainedKind(RowKind.DELETE)
				.build();
	}
}
