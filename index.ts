import { Database, type SQLQueryBindings } from "bun:sqlite";
import {
  CompiledQuery,
  DEFAULT_MIGRATION_LOCK_TABLE,
  DEFAULT_MIGRATION_TABLE,
  DefaultQueryCompiler,
  SelectQueryNode,
  sql,
  DialectAdapterBase,
} from "kysely";
import type {
  DatabaseConnection,
  Dialect,
  Driver,
  QueryCompiler,
  DialectAdapter,
  Kysely,
  QueryResult,
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
} from "kysely";

/**
 * Config for the SQLite dialect.
 */
export interface BunSqliteDialectConfig {
  /**
   * An bun:sqlite instance or a function that returns one.
   */
  database: Database;

  /**
   * Called once when the first query is executed.
   */
  onCreateConnection?: (connection: DatabaseConnection) => Promise<void>;
}

export class BunSqliteDialect implements Dialect {
  readonly #config: BunSqliteDialectConfig;

  constructor(config: BunSqliteDialectConfig) {
    this.#config = { ...config };
  }

  createDriver(): Driver {
    return new BunSqliteDriver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new BunSqliteQueryCompiler();
  }

  createAdapter(): DialectAdapter {
    return new BunSqliteAdapter();
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new BunSqliteIntrospector(db);
  }
}

export class BunSqliteDriver implements Driver {
  readonly #config: BunSqliteDialectConfig;
  readonly #connectionMutex = new ConnectionMutex();

  #db?: Database;
  #connection?: DatabaseConnection;

  constructor(config: BunSqliteDialectConfig) {
    this.#config = { ...config };
  }

  async init(): Promise<void> {
    this.#db = this.#config.database;

    this.#connection = new BunSqliteConnection(this.#db);

    if (this.#config.onCreateConnection !== undefined) {
      await this.#config.onCreateConnection(this.#connection);
    }
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    // SQLite only has one single connection. We use a mutex here to wait
    // until the single connection has been released.
    await this.#connectionMutex.lock();
    return this.#connection!;
  }

  async releaseConnection(): Promise<void> {
    this.#connectionMutex.unlock();
  }

  async beginTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("begin"));
  }

  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("commit"));
  }

  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("rollback"));
  }

  async destroy(): Promise<void> {
    this.#db?.close();
  }
}

class BunSqliteConnection implements DatabaseConnection {
  readonly #db: Database;

  constructor(db: Database) {
    this.#db = db;
  }

  executeQuery<O>({
    sql,
    query,
    parameters,
  }: CompiledQuery): Promise<QueryResult<O>> {
    const method = SelectQueryNode.is(query) ? "query" : "prepare";
    const stmt = this.#db[method]<O, SQLQueryBindings[]>(sql);

    const rows = stmt.all(...((parameters || []) as SQLQueryBindings[]));
    if (method === "query") {
      return Promise.resolve({
        rows: stmt.all(...((parameters || []) as SQLQueryBindings[])),
      });
    }

    const result = this.#db
      .query<
        {
          insertId: number | bigint;
          numAffectedRows: number | bigint;
        },
        []
      >(
        `select last_insert_rowid() as "insertId", changes() as "numAffectedRows"`
      )
      .get() ?? {
      insertId: 0n,
      numAffectedRows: 0n,
    };

    return Promise.resolve({
      rows,
      insertId: BigInt(result.insertId),
      numAffectedRows: BigInt(result.numAffectedRows),
    });
  }

  async *streamQuery<O>({
    sql,
    query,
    parameters,
  }: CompiledQuery): AsyncIterableIterator<QueryResult<O>> {
    const method = SelectQueryNode.is(query) ? "query" : "prepare";
    const stmt = this.#db[method]<O, SQLQueryBindings[]>(sql);
    for (const row of stmt.iterate(
      ...((parameters || []) as SQLQueryBindings[])
    )) {
      yield { rows: [row] };
    }
  }
}

class ConnectionMutex {
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
    const resolve = this.#resolve;

    this.#promise = undefined;
    this.#resolve = undefined;

    resolve?.();
  }
}

export class BunSqliteQueryCompiler extends DefaultQueryCompiler {
  protected override getCurrentParameterPlaceholder() {
    return "?";
  }

  protected override getLeftIdentifierWrapper(): string {
    return '"';
  }

  protected override getRightIdentifierWrapper(): string {
    return '"';
  }

  protected override getAutoIncrement() {
    return "autoincrement";
  }
}

export class BunSqliteIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<any>;

  constructor(db: Kysely<any>) {
    this.#db = db;
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    return [];
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false }
  ): Promise<TableMetadata[]> {
    let query = this.#db
      .selectFrom("sqlite_schema")
      .where("type", "=", "table")
      .where("name", "not like", "sqlite_%")
      .select("name")
      .$castTo<{ name: string }>();

    if (!options.withInternalKyselyTables) {
      query = query
        .where("name", "!=", DEFAULT_MIGRATION_TABLE)
        .where("name", "!=", DEFAULT_MIGRATION_LOCK_TABLE);
    }

    const tables = await query.execute();
    return Promise.all(tables.map(({ name }) => this.#getTableMetadata(name)));
  }

  async getMetadata(
    options?: DatabaseMetadataOptions
  ): Promise<DatabaseMetadata> {
    return {
      tables: await this.getTables(options),
    };
  }

  async #getTableMetadata(table: string): Promise<TableMetadata> {
    const db = this.#db;

    // Get the SQL that was used to create the table.
    const createSql = await db
      .selectFrom("sqlite_master")
      .where("name", "=", table)
      .select("sql")
      .$castTo<{ sql: string | undefined }>()
      .execute();

    // Try to find the name of the column that has `autoincrement`.
    const autoIncrementCol = createSql[0]?.sql
      ?.split(/[\(\),]/)
      ?.find((it) => it.toLowerCase().includes("autoincrement"))
      ?.split(/\s+/)?.[0]
      ?.replace(/["`]/g, "");

    const columns = await db
      .selectFrom(
        sql<{
          name: string;
          type: string;
          notnull: 0 | 1;
          dflt_value: any;
        }>`pragma_table_info(${table})`.as("table_info")
      )
      .select(["name", "type", "notnull", "dflt_value"])
      .execute();

    return {
      name: table,
      columns: columns.map((col) => ({
        name: col.name,
        dataType: col.type,
        isNullable: !col.notnull,
        isAutoIncrementing: col.name === autoIncrementCol,
        hasDefaultValue: col.dflt_value != null,
      })),
      isView: true,
    };
  }
}

export class BunSqliteAdapter extends DialectAdapterBase {
  override get supportsCreateIfNotExists(): boolean {
    return true;
  }

  override get supportsTransactionalDdl(): boolean {
    return true;
  }

  override get supportsReturning(): boolean {
    return true;
  }

  override get supportsOutput(): boolean {
    return false;
  }

  override async acquireMigrationLock(): Promise<void> {
    // SQLite only has one connection that's reserved by the migration system
    // for the whole time between acquireMigrationLock and releaseMigrationLock.
    // We don't need to do anything here.
  }

  override async releaseMigrationLock(): Promise<void> {
    // SQLite only has one connection that's reserved by the migration system
    // for the whole time between acquireMigrationLock and releaseMigrationLock.
    // We don't need to do anything here.
  }
}
