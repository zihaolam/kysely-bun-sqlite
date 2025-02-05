# kysely-bun-sqlite

A fork of [kysely-bun-sqlite](https://github.com/dylanblokhuis/kysely-bun-sqlite) with some improvements.

More explicitly, this fork:

1. adds support for streaming query results,
2. adds support for `numAffectedRows` for `INSERT`, `UPDATE`, and `DELETE` queries, and
3. caches `SELECT` queries.

## Installation

```bash
bun i https://github.com/lithdew/kysely-bun-sqlite
```
