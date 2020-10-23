package xyz.jianzha.kafkaStream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author Y_Kevin
 * @date 2020-10-21 18:56
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        // 核心处理流程
        String input = new String(line);
        // 提取数据，以固定前缀过滤日志信息
        if (input.contains("PRODUCT_RATING_PREFIX:")) {
            System.out.println("product rating coming!!!!" + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long timestamp) {
    }

    @Override
    public void close() {
    }
}
