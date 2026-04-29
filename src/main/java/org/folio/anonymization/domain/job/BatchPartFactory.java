package org.folio.anonymization.domain.job;

import org.jooq.Condition;

/**
 * Factory for creating parts based on a batch range. Given a jOOQ condition to match the range and an
 * integer to start with (based on the range). It is guaranteed that this value on the range of
 * [value, value + count(condition)) is exclusive to this part and can be used as the basis for unique
 * values. The end value is the maximum for convenience, equal to min(value + batch_size, total)
 */
@FunctionalInterface
public interface BatchPartFactory {
  public JobPart build(String label, Condition condition, int startIndexInclusive, int endIndexExclusive);
}
