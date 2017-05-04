package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.LWDocument;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lucidworks.hadoop.hive.ArrayProcessor.resolveList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.copyToStandardJavaObject;

public class MapProcessor {

  public static void resolve(boolean enableFieldMapping, LWDocument doc, String docFieldName, Object data,
    StructField structField, StructObjectInspector inspector) {
    MapObjectInspector moi = (MapObjectInspector) structField.getFieldObjectInspector();
    Object mapValue = inspector.getStructFieldData(data, structField);
    Map<Object, Object> map = (Map<Object, Object>) copyToStandardJavaObject(mapValue, moi);
    Map<String, Object> entries = new HashMap<>();
    resolveMap(enableFieldMapping, entries, docFieldName, map);

    for (Map.Entry<String, Object> entry : entries.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue());
    }

    entries.clear();
  }

  public static void resolveMap(boolean enableFieldMapping, Map<String, Object> entries, String docFieldName,
    Map<Object, Object> map) {
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      try {
        Object item = entry.getValue();
        String fieldName = docFieldName + "." + entry.getKey();

        if (!(item instanceof List || item instanceof Map)) {
          if (enableFieldMapping) {
            fieldName = FieldMappingHelper.fieldMapping(fieldName, item);
          }
          entries.put(fieldName, item);
        } else if (item instanceof List) {
          resolveList(enableFieldMapping, entries, fieldName, (List<Object>) item, -1);
        } else if (item instanceof Map) {
          resolveMap(enableFieldMapping, entries, fieldName, (Map<Object, Object>) item);
        } else {
          continue;
        }
      } catch (SerDeException e) {
        continue;
      }
    }
  }
}
