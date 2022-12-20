package org.joisen;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joisen.func.CustomerDeserializationSchema;

/**
 * @Author Joisen
 * @Date 2022/12/20 17:07
 * @Version 1.0
 */
public class FlinkCDC2 {
    public static void main(String[] args) throws Exception {
        // 1 互殴去flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 1.1 开启cp
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/cp1"));

        // 2 通过flinkCDC 构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("999719")
                .databaseList("cdc_test") // 可以监控多个数据库
                .tableList("cdc_test.user_info") // 监控多个表，表名加上库名, 如果不加该内容则读取当前数据库的所有表内容
                .deserializer( new CustomerDeserializationSchema() )
                .startupOptions( StartupOptions.earliest() )
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        // 3 数据打印
        dataStreamSource.print();
        // 4 启动任务
        env.execute(" FlinkCDC1 ");

    }
}
