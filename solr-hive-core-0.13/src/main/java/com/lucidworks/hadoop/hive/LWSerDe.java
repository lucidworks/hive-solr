package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LWSerDe implements SerDe {

  private static final Logger LOG = LoggerFactory.getLogger(LWSerDe.class);

  protected StructTypeInfo typeInfo;
  protected ObjectInspector inspector;
  protected List<String> colNames;
  protected List<TypeInfo> colTypes;
  protected List<Object> row;
  private JobConf conf;

  @Override
  public void initialize(Configuration conf, Properties tblProperties) throws SerDeException {

    colNames = Arrays.asList(tblProperties.getProperty(Constants.LIST_COLUMNS).split(","));
    colTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(tblProperties.getProperty(Constants.LIST_COLUMN_TYPES));
    typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
    inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    row = new ArrayList<Object>();

    try {
      LWDocumentProvider.init((JobConf) conf);
      this.conf = (JobConf) conf;
    } catch (Exception e) {
      LOG.warn("LWDocumentFactoryHandler not initialize");
    }
  }

  @Override
  public Object deserialize(Writable data) throws SerDeException {
    if (!(data instanceof LWDocumentWritable)) {
      return null;
    }

    row.clear();
    LWDocument doc = ((LWDocumentWritable) data).getLWDocument();

    for (String fieldName : typeInfo.getAllStructFieldNames()) {
      if (fieldName.equalsIgnoreCase("id")) {
        String id = doc.getId();
        if (id != null) {
          row.add(doc.getId());
          continue;
        }
      }
      // Just add first element for now
      Object firstField = doc.getFirstFieldValue(fieldName);
      if (firstField != null) {
        row.add(firstField);

      }
    }

    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return LWDocumentWritable.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // Nothing for now
    return null;
  }

  @Override
  public Writable serialize(Object data, ObjectInspector objInspector) throws SerDeException {

    // Make sure we have a struct, as Hive "root" fields should be a struct
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException("Unable to serialize root type of " + objInspector.getTypeName());
    }

    // Our doc
    LWDocument doc = LWDocumentProvider.createDocument();

    // Fields...
    StructObjectInspector inspector = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      StructField f = fields.get(i);
      String docFieldName = colNames.get(i);

      if (docFieldName.equalsIgnoreCase("id")) {
        if (f.getFieldObjectInspector().getCategory() == Category.PRIMITIVE) {
          Object id = inspector.getStructFieldData(data, f);
          doc.setId(id.toString()); // We're making a lot of assumption here that this is a string

        } else {
          throw new SerDeException("id field must be a primitive [String] type");
        }

      } else {
        switch (f.getFieldObjectInspector().getCategory()) {
          case PRIMITIVE:
            Object value = ObjectInspectorUtils
                .copyToStandardJavaObject(inspector.getStructFieldData(data, f),
                    f.getFieldObjectInspector());
            doc.addField(docFieldName, value);
            break;

          case STRUCT:
          case MAP:
          case LIST:
          case UNION:
            throw new SerDeException(
                "We don't yet support nested types (found " + f.getFieldObjectInspector()
                    .getTypeName() + ")");
        }
      }
    }

    return new LWDocumentWritable(doc);
  }
}
