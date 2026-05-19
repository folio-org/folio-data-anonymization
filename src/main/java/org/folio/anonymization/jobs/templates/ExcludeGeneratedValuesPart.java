package org.folio.anonymization.jobs.templates;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Field;
import org.jooq.Table;

/**
 * Job part to remove generated values for a field in a table. Designed to remove generated values
 * for ones that need to be preserved (e.g. system users' usernames when anonymizing usernames).
 * Replaces newValue with originalValue where originalValue IN (list).
 *
 * It is expected that the provided list is relatively small.
 */
@Log4j2
public class ExcludeGeneratedValuesPart extends JobPart {

  private final Table<?> destinationTable;
  private final Field<String> originalValue;
  private final Field<String> newValue;
  private final List<String> valuesToPreserve;

  public ExcludeGeneratedValuesPart(
    String label,
    Table<?> destinationTable,
    Field<String> originalValue,
    Field<String> newValue,
    List<String> valuesToPreserve
  ) {
    super(label);
    this.destinationTable = destinationTable;
    this.originalValue = originalValue;
    this.newValue = newValue;
    this.valuesToPreserve = valuesToPreserve;
  }

  @Override
  protected void execute() {
    this.create()
      .update(destinationTable)
      .set(newValue, originalValue)
      .where(originalValue.in(valuesToPreserve))
      .execute();
  }
}
