package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {
	
	// 定义所有配置项
	public static final ConfigOption<String> COLUMN_DELIMITER = ConfigOptions.key("column-delimiter")
			.stringType()
			.defaultValue("|");
	
	
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		// 使用提供的工具类或实现你自己的逻辑进行校验
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		
		// 获取校验完的配置项
		final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);
		
		// 创建并返回解码器
		return new ChangelogCsvFormat(columnDelimiter);
	}
	
	@Override
	public String factoryIdentifier() {
		return "changelog-csv";
	}
	
	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}
	
	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(COLUMN_DELIMITER);
		return options;
	}
}
