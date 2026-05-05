package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader.SeedValue;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.springframework.core.io.Resource;

public class ReplaceProfilePicturesFromSeedPart extends JobPart {

  private static final String ROTATING_REPLACEMENT_SQL =
    """
    update {0} as p
    set profile_picture_blob = s.profile_picture_blob,
        hmac = s.hmac
    from (
      select id, ((row_number() over (order by id) - 1 + {3}) % {2})::int as row_num
      from {0}
      where profile_picture_blob is not null and hmac is not null
    ) ord
    join {1} s on s.row_num = ord.row_num
    where p.id = ord.id
    """;

  private static final int INSERT_BATCH_SIZE = 100;

  private final Resource seedCsvResource;

  public ReplaceProfilePicturesFromSeedPart(String label, Resource seedCsvResource) {
    super(label);
    this.seedCsvResource = seedCsvResource;
  }

  @Override
  protected void execute() {
    List<SeedValue> seeds = ProfilePictureSeedCsvLoader.load(seedCsvResource);
    int seedCount = seeds.size();

    String schemaName = DBUtils.getSchemaName(this.tenant().id(), "users");
    Table<?> profilePictureTable = table(name(schemaName, "profile_picture"));

    Field<Integer> rowNum = field("row_num", SQLDataType.INTEGER.notNull());
    Field<byte[]> profilePictureBlob = field("profile_picture_blob", SQLDataType.BLOB.nullable(true));
    Field<byte[]> hmac = field("hmac", SQLDataType.BLOB.nullable(true));

    this.create()
      .transaction(configuration -> {
        DSLContext ctx = using(configuration);

        int runOffset = seedCount > 1 ? ThreadLocalRandom.current().nextInt(seedCount) : 0;

        Table<?> tempSeedTable = table(name("_danon_profile_picture_seeds_" + System.nanoTime()));
        ctx
          .createTemporaryTable(tempSeedTable)
          .columns(rowNum, profilePictureBlob, hmac)
          .primaryKey(rowNum)
          .onCommitDrop()
          .execute();

        List<Query> inserts = new ArrayList<>(seeds.size());
        for (int i = 0; i < seeds.size(); i++) {
          SeedValue seed = seeds.get(i);
          inserts.add(
            ctx.insertInto(tempSeedTable).columns(rowNum, profilePictureBlob, hmac).values(i, seed.profilePictureBlob(), seed.hmac())
          );
        }
        for (int i = 0; i < inserts.size(); i += INSERT_BATCH_SIZE) {
          int end = Math.min(i + INSERT_BATCH_SIZE, inserts.size());
          ctx.batch(inserts.subList(i, end)).execute();
        }

        ctx.execute(
          ROTATING_REPLACEMENT_SQL,
          profilePictureTable,
          tempSeedTable,
          seedCount,
          runOffset
        );
      });
  }
}
