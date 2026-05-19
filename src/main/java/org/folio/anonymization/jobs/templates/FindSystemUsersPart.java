package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.not;

import java.util.List;
import java.util.function.Consumer;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.SystemUserExclusionUtil;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

public class FindSystemUsersPart extends JobPart {

  private final Table<?> table;
  private final Field<String> field;
  // we expect a low number of these (less than number of total modules), so we do not need to batch
  private final Consumer<List<String>> handler;

  public FindSystemUsersPart(String label, Table<?> table, Field<String> field, Consumer<List<String>> handler) {
    super(label);
    this.table = table;
    this.field = field;
    this.handler = handler;
  }

  @Override
  public void execute() {
    Condition condition = not(SystemUserExclusionUtil.getExclusionCondition(this.tenant()));

    handler.accept(this.create().select(field).from(table).where(condition.and(field.isNotNull())).fetch(field));
  }
}
