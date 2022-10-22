package top.onepiece.feng.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * 自定义序列化数据
 *
 * @author fengyafei
 */
public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

  public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
    String topic = sourceRecord.topic();
    String[] dbAndTables = topic.split("\\.");
    String dbName = dbAndTables[1];
    String tableName = dbAndTables[2];

    Envelope.Operation operation = Envelope.operationFor(sourceRecord);

    Struct struct = (Struct) sourceRecord.value();
    //  获取变化后的数据
    Struct after = struct.getStruct("after");
    JSONObject afterJson = new JSONObject();
    if (null != after) {
      List<Field> fields = after.schema().fields();
      for (Field field : fields) {
        afterJson.put(field.name(), after.get(field));
      }
    }

    // 获取变化前的数据
    Struct before = struct.getStruct("before");
    JSONObject beforeJson = new JSONObject();
    if (null != before) {
      List<Field> fields = before.schema().fields();
      for (Field field : fields) {
        beforeJson.put(field.name(), before.get(field));
      }
    }

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("dbName", dbName);
    jsonObject.put("tableName", tableName);
    jsonObject.put("before", beforeJson);
    jsonObject.put("after", afterJson);
    jsonObject.put("op", operation.name());

    collector.collect(jsonObject.toJSONString());
  }

  public TypeInformation<String> getProducedType() {
    return TypeInformation.of(String.class);
  }
}
