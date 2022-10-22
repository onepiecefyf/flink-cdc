package top.onepiece.feng.flink.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink cdc 测试案例
 *
 * Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点 续传,需要从 Checkpoint 或者 Savepoint 启动程序
 *
 * 开启checkPoint 每5分钟检查一次
 * environment.enableCheckpointing(5000);
 * 指定CK的一致性语义
 * environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
 * 设置任务关闭的时候保留最后一次CK数据
 * environment .getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); //
 * 指定从CK重启策略 重启三次 每次间隔2s
 * environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));
 * 设置状态后端 environment.setStateBackend(new
 * FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
 * 设置访问 HDFS 的用户名
 * System.setProperty("HADOOP_USER_NAME", "atguigu");
 *
 * @author fengyafei
 */
public class FlinkCdc {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    DebeziumSourceFunction<String> debeziumSourceFunction =
        MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3306)
            .username("root")
            .password("Feng1992")
            .databaseList("flink_cdc")
            .tableList("flink_cdc.cb_user")
            .startupOptions(StartupOptions.initial())
            .deserializer(new CustomDebeziumDeserializationSchema())
            .build();

    DataStreamSource<String> dataStreamSource = environment.addSource(debeziumSourceFunction);

    dataStreamSource.print();

    environment.execute();
  }
}
