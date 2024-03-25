package com.aierx;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author leiwenyong
 * @since 2024-03-25
 */
public class TimedSource implements SourceFunction<Long> {
	
	private boolean running = true;
	@Override
	public void run(SourceContext<Long> sourceContext) throws Exception {
	
	
		while (running){
			long ts = System.currentTimeMillis();
			ts = ts - ts % 1000;
			sourceContext.collect(ts);
			Thread.sleep(1000 - ts % 1000);
		}
	
	}
	
	@Override
	public void cancel() {
		this.running = false;
	}
}
