# ddlinfo

## Location
[src/bin/pgbench/pgbench.c:4836-4917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4836-L4917)

## Overview
The ddlinfo structure defines table schema information for pgbench's standard TPC-B benchmark tables, supporting both 32-bit and 64-bit account ID configurations.

## Definition
```c
struct ddlinfo
{
    const char *table;          /* table name */
    const char *smcols;         /* column decls if accountIDs are 32 bits */
    const char *bigcols;        /* column decls if accountIDs are 64 bits */
    int         declare_fillfactor;
};
```

## Detailed Description
This structure serves as a template for creating the four standard pgbench tables (pgbench_history, pgbench_tellers, pgbench_accounts, pgbench_branches) with appropriate column definitions based on the scale of the benchmark. The structure supports PostgreSQL's account ID scaling by providing different column definitions for 32-bit and 64-bit account identifiers, which becomes necessary when the scale factor exceeds the 32-bit threshold. The design maintains historical compatibility with TPC-B benchmarking standards while accommodating PostgreSQL-specific optimizations.

## Parameters / Member Variables
- `table`: Name of the pgbench table to be created (e.g., "pgbench_accounts", "pgbench_tellers")
- `smcols`: Column definition string used when account IDs fit in 32-bit integers (smaller scale factors)
- `bigcols`: Column definition string used when account IDs require 64-bit integers (large scale factors)
- `declare_fillfactor`: Boolean flag indicating whether this table should include a fillfactor specification during creation

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - lengthof
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - SCALE_32BIT_THRESHOLD
  - PART_NONE
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [executeStatement](../e/executeStatement.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [createPartitions](../c/createPartitions.md)
- Called from (representative examples):
  - DDLs array initialization
  - initCreateTables function

## Notes and Other Information
The structure is used within a static DDLs array that defines all four standard pgbench tables. While designed to comply with TPC-B requirements of at least 100 bytes per row, the implementation maintains historical behavior where most filler columns default to NULL rather than consuming actual space. The dual column definition approach (smcols/bigcols) ensures optimal performance for both small and large-scale benchmarks while maintaining data type consistency.