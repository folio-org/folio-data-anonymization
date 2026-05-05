package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.sequence;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

import java.util.List;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader.SeedValue;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Sequence;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;

public class ReplaceProfilePicturesFromSeedPart extends JobPart {

  private final String seedCsvPath;

  public ReplaceProfilePicturesFromSeedPart(String label, String seedCsvPath) {
    super(label);
    this.seedCsvPath = seedCsvPath;
  }

  @Override
  protected void execute() {
    List<SeedValue> seeds = ProfilePictureSeedCsvLoader.load(seedCsvPath);
    int seedCount = seeds.size();

    String schemaName = DBUtils.getSchemaName(this.tenant().id(), "users");
    Table<?> profilePictureTable = table(name(schemaName, "profile_picture"));

    Field<Integer> rowNum = field("row_num", SQLDataType.INTEGER.notNull());
    Field<byte[]> profilePictureBlob = field("profile_picture_blob", SQLDataType.BLOB.nullable(true));
    Field<byte[]> hmac = field("hmac", SQLDataType.BLOB.nullable(true));

    this.create()
      .transaction(configuration -> {
        DSLContext ctx = using(configuration);

        int runOffset = 0;
        if (seedCount > 1) {
          Sequence<Integer> runOffsetSequence = sequence(
            name("public", "_danon_profile_picture_run_offset_" + this.tenant().id() + "_seq"),
            SQLDataType.INTEGER
          );
          ctx.createSequenceIfNotExists(runOffsetSequence).startWith(0).minvalue(0).execute();
          Integer runCounter = ctx.fetchValue(runOffsetSequence.nextval());
          runOffset = Math.floorMod(runCounter, seedCount);
        }

        Table<?> tempSeedTable = table(name("_danon_profile_picture_seeds_" + System.nanoTime()));
        ctx
          .createTemporaryTable(tempSeedTable)
          .columns(rowNum, profilePictureBlob, hmac)
          .primaryKey(rowNum)
          .onCommitDrop()
          .execute();

        for (int i = 0; i < seeds.size(); i++) {
          SeedValue seed = seeds.get(i);
          ctx
            .insertInto(tempSeedTable)
            .columns(rowNum, profilePictureBlob, hmac)
            .values(i, seed.profilePictureBlob(), seed.hmac())
            .execute();
        }

        ctx.execute(
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
          """,
          profilePictureTable,
          tempSeedTable,
          seedCount,
          runOffset
        );
      });
  }
}
