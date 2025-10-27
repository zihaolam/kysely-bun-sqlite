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
        })),
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

class AsyncMutex {
  #promise?: Promise<void>;
  #resolve?: () => void;

  async lock(): Promise<void> {
    while (this.#promise) {
      await this.#promise;
    }

    const { promise, resolve } = Promise.withResolvers<void>();
    this.#promise = promise;
    this.#resolve = resolve;
  }

  unlock(): void {
    if (!this.#promise) {
      throw new Error("Mutex is not locked");
    }

    this.#resolve!();
    this.#promise = undefined;
    this.#resolve = undefined;
  }
}

test("transaction mutex", async () => {
  const mutex = new AsyncMutex();
  const dialect = new BunSqliteDialect({
    database: new Database(":memory:", { safeIntegers: true, strict: true }),
    transactionMutex: mutex,
  });

  const db = new Kysely<any>({ dialect });
  const unlockedAt = Date.now() + 200;

  // just lock here and unlock after 200ms
  // the transaction should wait for the unlock before proceeding, and created_at should be later than unlockedAt
  void mutex.lock();

  setTimeout(() => {
    mutex.unlock();
  }, 200);

  await db.schema
    .createTable("test")
    .addColumn("id", "integer", (b) => b.primaryKey().autoIncrement())
    .addColumn("created_at", "datetime", (b) => b.notNull())
    .execute();

  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto("test")
      .values({ id: 1, created_at: new Date().toISOString() })
      .executeTakeFirstOrThrow();
  });

  const data = await db
    .selectFrom("test")
    .select(["id", "created_at"])
    .executeTakeFirstOrThrow();

  // assert that the created_at is later than unlockedAt
  expect(Date.parse(data.created_at)).toBeGreaterThanOrEqual(unlockedAt);

  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto("test")
      .values({ id: 2, created_at: new Date().toISOString() })
      .executeTakeFirstOrThrow();
  });

  // testing that the mutex is properly released after the transaction
  // it should find the row with id 2
  expect(() =>
    db
      .selectFrom("test")
      .select(["id"])
      .where("id", "=", 2)
      .executeTakeFirstOrThrow(),
  ).not.toThrow();

  // throw here to cause a rollback
  expect(() =>
    db.transaction().execute(async (tx) => {
      await tx
        .insertInto("test")
        .values({ id: 3, created_at: new Date().toISOString() })
        .executeTakeFirstOrThrow();
      throw new Error("Rolling back transaction");
    }),
  ).toThrow();

  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto("test")
      .values({ id: 4, created_at: new Date().toISOString() })
      .executeTakeFirstOrThrow();
  });

  // testing that values can be inserted after a rollback to assert that mutex is released after rolblack
  expect(() =>
    db
      .selectFrom("test")
      .select(["id"])
      .where("id", "=", 4)
      .executeTakeFirstOrThrow(),
  ).not.toThrow();
});
