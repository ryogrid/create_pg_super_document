# append_rel_pattern_filtered_cte

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1844-1882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1844-L1882)

## Overview
Creates a filtered Common Table Expression that selects only database-relevant patterns from a raw pattern CTE for pg_amcheck relation processing.

## Definition

```c
static void
append_rel_pattern_filtered_cte(PQExpBuffer buf, const char *raw,
								const char *filtered, PGconn *conn)
```
## Detailed Description
This function generates a CTE that filters patterns from a raw pattern CTE to include only those relevant to the current database connection. It applies database-level filtering logic: patterns with no database component are always included, patterns whose database component matches the current connection's database are included, and patterns targeting other databases are excluded. Additionally, it filters out patterns that have neither namespace nor relation components, as these would be too broad to be useful for relation matching.

## Parameters / Member Variables
- `buf`: PQExpBuffer to which the filtered CTE SQL will be appended
- `raw`: Name of the source raw CTE containing unfiltered patterns (typically from append_rel_pattern_raw_cte)
- `filtered`: Name to assign to the new filtered CTE being created
- `conn`: PostgreSQL connection handle, used to determine the current database name for filtering

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBuffer](appendPQExpBuffer.md)
  - [appendStringLiteralConn](appendStringLiteralConn.md)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [PQdb](../P/PQdb.md)
- Called from (representative examples):
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (at src/bin/pg_amcheck/pg_amcheck.c:1902)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (at src/bin/pg_amcheck/pg_amcheck.c:1912)

## Notes and Other Information
- Generates a five-column filtered CTE: pattern_id, nsp_regex, rel_regex, heap_only, btree_only (drops db_regex column)
- Implements database-specific filtering: includes patterns with no database part OR patterns matching current database
- Excludes overly broad patterns (those with neither namespace nor relation components)
- Uses proper SQL string literal escaping for the database name to prevent injection
- Example: Connected to 'foo', patterns 'foo.bar.baz' and 'alpha.beta' are included, 'other_db.schema.table' is excluded
- Essential for ensuring patterns only apply to relations in the currently connected database
- Part of the multi-stage filtering process in pg_amcheck's relation discovery system