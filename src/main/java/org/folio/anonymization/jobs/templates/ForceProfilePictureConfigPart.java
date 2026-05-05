package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.condition;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.JSONB;
import org.jooq.Table;

public class ForceProfilePictureConfigPart extends JobPart {

  private final String encryptionKey;

  public ForceProfilePictureConfigPart(String label, String encryptionKey) {
    super(label);
    this.encryptionKey = encryptionKey;
  }

  @Override
  protected void execute() {
    String schemaName = DBUtils.getSchemaName(this.tenant().id(), "users");
    Table<?> settingsTable = table(name(schemaName, "settings"));

    var jsonbField = field(name(schemaName, "settings", "jsonb"), JSONB.class);
    var updatedJsonb = field(
      """
      jsonb_set(
        jsonb_set(
          jsonb_set(
            {0},
            '{value,encryptionKey}', to_jsonb(cast({1} as text)), true
          ),
          '{value,enabledObjectStorage}', to_jsonb(cast(false as boolean)), true
        ),
        '{value,enabled}', to_jsonb(cast(true as boolean)), true
      )
      """,
      JSONB.class,
      jsonbField,
      inline(encryptionKey)
    );

    int updatedRows = this.create()
      .update(settingsTable)
      .set(jsonbField, updatedJsonb)
      .where(condition("{0} ->> 'key' = 'PROFILE_PICTURE_CONFIG' and {0} ->> 'scope' = 'mod-users'", jsonbField))
      .execute();

    if (updatedRows == 0) {
      throw new IllegalStateException(
        "PROFILE_PICTURE_CONFIG not found in users.settings for tenant " + this.tenant().id()
      );
    }
  }
}

