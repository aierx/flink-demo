package com.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
	
	private final String hostname;
	private final int port;
	private final byte byteDelimiter;
	private final DeserializationSchema<RowData> deserializer;
	
	private volatile boolean isRunning = true;
	private Socket currentSocket;
	
	public SocketSourceFunction(String hostname, int port, byte byteDelimiter, DeserializationSchema<RowData> deserializer) {
		this.hostname = hostname;
		this.port = port;
		this.byteDelimiter = byteDelimiter;
		this.deserializer = deserializer;
	}
	
	@Override
	public TypeInformation<RowData> getProducedType() {
		return deserializer.getProducedType();
	}
	
	@Override
	public void run(SourceContext<RowData> ctx) throws Exception {
		while (isRunning) {
			// 持续从 socket 消费数据
			try (final Socket socket = new Socket()) {
				currentSocket = socket;
				socket.connect(new InetSocketAddress(hostname, port), 0);
				try (InputStream stream = socket.getInputStream()) {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					int b;
					while ((b = stream.read()) >= 0) {
						// 持续写入 buffer 直到遇到分隔符
						if (b != byteDelimiter) {
							buffer.write(b);
						}
						// 解码并处理记录
						else {
							ctx.collect(deserializer.deserialize(buffer.toByteArray()));
							buffer.reset();
						}
					}
				}
			} catch (Throwable t) {
				t.printStackTrace(); // 打印并继续
			}
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		try {
			currentSocket.close();
		} catch (Throwable t) {
			// 忽略
		}
	}
}
