package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class SocketDynamicTableFactory implements DynamicTableSourceFactory {
	
	// 定义所有配置项
	public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
			.stringType()
			.noDefaultValue();
	
	public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
			.intType()
			.noDefaultValue();
	
	public static final ConfigOption<Integer> BYTE_DELIMITER = ConfigOptions.key("byte-delimiter")
			.intType()
			.defaultValue(10); // 等同于 '\n'
	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
// 使用提供的工具类或实现你自己的逻辑进行校验
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		
		// 找到合适的解码器
		final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		
		// 校验所有的配置项
		helper.validate();
		
		// 获取校验完的配置项
		final ReadableConfig options = helper.getOptions();
		final String hostname = options.get(HOSTNAME);
		final int port = options.get(PORT);
		final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);
		
		// 从 catalog 中抽取要生产的数据类型 (除了需要计算的列)
		final DataType producedDataType =
				context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
		
		// 创建并返回动态表 source
		return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
	}
	
	@Override
	public String factoryIdentifier() {
		return "socket";
	}
	
	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOSTNAME);
		options.add(PORT);
		options.add(FactoryUtil.FORMAT); // 解码的格式器使用预先定义的配置项
		return options;
	}
	
	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(BYTE_DELIMITER);
		return options;
	}
}
