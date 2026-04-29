package org.folio.anonymization.jobs.templates;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

/**
 * Job part to create unique data for a field in a table. Designed to be used with temporary tables,
 * so we use a regular jOOQ Field
 */
@Log4j2
public class GenerateUniqueValuesPart extends JobPart {

  private static final int INSERT_BATCH_SIZE = 100;

  private final Table<?> destinationTable;
  private final Field<String> destinationField;
  private final Select<?> source;
  private final Function<Integer, List<String>> valueFactory;

  public GenerateUniqueValuesPart(
    String label,
    Table<?> destinationTable,
    Field<String> destinationField,
    Select<?> source,
    Function<Integer, List<String>> valueFactory
  ) {
    super(label);
    this.destinationTable = destinationTable;
    this.destinationField = destinationField;
    this.source = source;
    this.valueFactory = valueFactory;
  }

  @Override
  protected void execute() {
    Result<?> toCopy = this.create().selectFrom(source).fetch();

    List<String> newValues = valueFactory.apply(toCopy.size());
    log.info("Found {} records to copy with new values", toCopy.size());

    List<Field<?>> sourceFields = Arrays.asList(source.fields());

    List<Query> queries = IntStream
      .range(0, toCopy.size())
      .mapToObj(i ->
        this.create()
          .insertInto(destinationTable, Stream.concat(sourceFields.stream(), Stream.of(destinationField)).toList())
          .values(
            Stream.concat(sourceFields.stream().map(f -> toCopy.get(i).get(f)), Stream.of(newValues.get(i))).toList()
          )
      )
      .map(Query.class::cast)
      .toList();

    for (int i = 0; i < queries.size(); i += INSERT_BATCH_SIZE) {
      int end = Math.min(i + INSERT_BATCH_SIZE, queries.size());
      List<Query> batch = queries.subList(i, end);
      this.create().batch(batch).execute();
    }
  }
}
