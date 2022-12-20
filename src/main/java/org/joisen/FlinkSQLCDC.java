package org.joisen;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author Joisen
 * @Date 2022/12/20 19:39
 * @Version 1.0
 */
public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        // 1 互殴去flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2 使用FlinkSQL DDL模式构建CDC表
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT primary key," +
                " name STRING," +
                " sex STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'hadoop102'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '999719'," +
                " 'database-name' = 'cdc_test'," +
                " 'table-name' = 'user_info'" +
                ")");

        // 3 查询数据并转换为流输出
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();


        env.execute();
    }
}
