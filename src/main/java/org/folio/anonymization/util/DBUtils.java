package org.folio.anonymization.util;

import static org.jooq.impl.DSL.jsonbGetAttribute;
import static org.jooq.impl.DSL.jsonbGetAttributeAsText;

import java.util.List;
import lombok.experimental.UtilityClass;
import org.jooq.Field;
import org.jooq.JSONB;

@UtilityClass
public class DBUtils {

  public static final String MODULE_SCHEMA_NAME_FORMAT = "%s_mod_%s";

  public static String getSchemaName(String tenantName, String normalizedModuleName) {
    return MODULE_SCHEMA_NAME_FORMAT.formatted(tenantName, normalizedModuleName);
  }

  public static Field<JSONB> resolveFieldProperties(Field<JSONB> field, List<String> path) {
    for (int i = 0; i < path.size(); i++) {
      field = jsonbGetAttribute(field, path.get(i));
    }
    return field;
  }

  public static Field<String> resolveFieldPropertiesToString(Field<JSONB> field, List<String> path) {
    String lastPath = path.get(path.size() - 1);

    return jsonbGetAttributeAsText(resolveFieldProperties(field, path.subList(0, path.size() - 1)), lastPath);
  }
}
