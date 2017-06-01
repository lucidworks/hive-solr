package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.LWDocument;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lucidworks.hadoop.hive.MapProcessor.resolveMap;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.copyToStandardJavaObject;

public class ArrayProcessor {

  public static void resolve(boolean enableFieldMapping, LWDocument doc, String docFieldName, Object data,
    StructField structField, StructObjectInspector inspector) throws Exception {
    ListObjectInspector loi = (ListObjectInspector) structField.getFieldObjectInspector();
    Object listValue = inspector.getStructFieldData(data, structField);
    List<Object> list = (List<Object>) copyToStandardJavaObject(listValue, loi);
    Map<String, Object> entries = new HashMap<>();
    resolveList(enableFieldMapping, entries, docFieldName, list, -1);

    for (Map.Entry<String, Object> entry : entries.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue());
    }

    entries.clear();
  }

  public static void resolveList(boolean enableFieldMapping, Map<String, Object> entries, String docFieldName,
    List<Object> list, int index) throws SerDeException {

    if (list.size() == 0) {
      throw new SerDeException("Empty list, cannot continue");
    }

    Object toAnalyze = list.get(0);
    boolean root = index >= 0;
    String fieldName = root ? docFieldName + "." + index : docFieldName;

    if (!(toAnalyze instanceof List || toAnalyze instanceof Map)) {
      if (root) {
        fieldName = fieldName + ".item";
      }

      if (enableFieldMapping) {
        fieldName = FieldMappingHelper.multiFieldMapping(fieldName, toAnalyze);
      }

      entries.put(fieldName, list);
    } else {
      if (toAnalyze instanceof List) {
        for (int currentIndex = 0; currentIndex < list.size(); currentIndex++) {
          resolveList(enableFieldMapping, entries, fieldName, (List<Object>) list.get(currentIndex), currentIndex);
        }
      } else if (toAnalyze instanceof Map) {
        for (int currentIndex = 0; currentIndex < list.size(); currentIndex++) {
          resolveMap(enableFieldMapping, entries, fieldName + "." + currentIndex,
            (Map<Object, Object>) list.get(currentIndex));
        }
      } else {
        throw new SerDeException("Invalid complex type: " + toAnalyze.getClass());
      }
    }
  }
}
