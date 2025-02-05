import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { Kysely } from "kysely";
import { BunSqliteDialect } from ".";

test("num affected rows", async () => {
  const dialect = new BunSqliteDialect({
    database: new Database(":memory:", { safeIntegers: true, strict: true }),
  });

  const db = new Kysely<any>({ dialect });

  await db.schema
    .createTable("test")
    .addColumn("id", "integer", (b) => b.primaryKey().autoIncrement())
    .addColumn("condition", "boolean", (b) => b.notNull())
    .execute();

  // Insert 100 rows.

  for (let i = 0; i < 100; i++) {
    await db
      .insertInto("test")
      .values({ condition: true })
      .executeTakeFirstOrThrow();
  }

  // Update 10 rows. Test that CONFLICT handling works when
  // calculating the number of updated rows.

  for (let id = 1n; id <= 10n; id++) {
    const ignored = await db
      .insertInto("test")
      .values({ id, condition: false })
      .onConflict((e) => e.doNothing())
      .executeTakeFirstOrThrow();

    expect(ignored.numInsertedOrUpdatedRows).toBe(0n);

    const updated = await db
      .insertInto("test")
      .values({ id, condition: false })
      .onConflict((e) =>
        e.doUpdateSet((e) => ({
          condition: e.ref("excluded.condition"),
        }))
      )
      .executeTakeFirstOrThrow();

    expect(updated.numInsertedOrUpdatedRows).toBe(1n);
  }

  // Update the 90 remaining rows where "condition" = true.
  // Test that the number of updated rows is calculated correctly.

  const updated = await db
    .updateTable("test")
    .set({ condition: false })
    .where("condition", "=", true)
    .executeTakeFirstOrThrow();

  expect(updated.numUpdatedRows).toBe(90n);
});
