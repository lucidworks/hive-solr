package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.lucidworks.hadoop.hive.HiveSolrConstants.ENABLE_FIELD_MAPPING;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.copyToStandardJavaObject;

// deprecation -> SerDe
@SuppressWarnings("deprecation")
public class LWSerDe implements SerDe {

  private static final Logger LOG = LoggerFactory.getLogger(LWSerDe.class);

  protected StructTypeInfo typeInfo;
  protected ObjectInspector inspector;
  protected List<String> colNames;
  protected List<TypeInfo> colTypes;
  protected List<Object> row;
  protected boolean enableFieldMapping;

  @Override
  public void initialize(Configuration conf, Properties tblProperties) throws SerDeException {
    colNames = Arrays.asList(tblProperties.getProperty(Constants.LIST_COLUMNS).split(","));
    colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tblProperties.getProperty(Constants.LIST_COLUMN_TYPES));
    typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
    inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    row = new ArrayList<>();
    enableFieldMapping = Boolean.valueOf(tblProperties.getProperty(ENABLE_FIELD_MAPPING, "false"));
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
    boolean existsId = false;

    for (int i = 0; i < fields.size(); i++) {
      StructField structField = fields.get(i);
      String docFieldName = colNames.get(i);

      if (docFieldName.equalsIgnoreCase("id")) {
        if (structField.getFieldObjectInspector().getCategory() == Category.PRIMITIVE) {
          Object id = inspector.getStructFieldData(data, structField);
          doc.setId(id.toString()); // We're making a lot of assumption here that this is a string

        } else {
          throw new SerDeException("id field must be a primitive [String] type");
        }

        existsId = true;

      } else {
        ObjectInspector foi = structField.getFieldObjectInspector();
        Category foiCategory = foi.getCategory();
        Object structFieldData = inspector.getStructFieldData(data, structField);
        Object value = copyToStandardJavaObject(structFieldData, structField.getFieldObjectInspector());
        if (foiCategory.equals(Category.PRIMITIVE)) {
          try {
            String fieldName = docFieldName;

            if(enableFieldMapping) {
              fieldName = FieldMappingHelper.fieldMapping(fieldName, value);
            }

            doc.addField(fieldName, value);
          } catch (Exception e) {
            continue;
          }

        } else if (foiCategory.equals(Category.LIST)) {
          try {
            ArrayProcessor.resolve(enableFieldMapping, doc, docFieldName, data, structField, inspector);
          } catch (Exception e) {
            continue;
          }
        } else if (foiCategory.equals(Category.MAP)) {
          try {
            MapProcessor.resolve(enableFieldMapping, doc, docFieldName, data, structField, inspector);
          } catch (Exception e) {
            continue;
          }
        } else {
          continue;
        }
      }
    }

    if (!existsId) {
      doc.setId(String.valueOf(UUID.randomUUID()));
    }

    return new LWDocumentWritable(doc);
  }
}
