# get_next_possible_free_pg_type_oid

## Location
src/bin/pg_dump/pg_dump.c: 5345 - 5375

## Overview
Finds and returns the next available OID in the pg_type system catalog that can be safely assigned to a new type during binary upgrades.

## Definition
```c
static Oid get_next_possible_free_pg_type_oid(Archive *fout, PQExpBuffer upgrade_query)
```

## Detailed Description
This function is used during binary upgrades in pg_dump to handle cases where PostgreSQL version differences require assignment of new type OIDs. Specifically, it addresses situations where an older PostgreSQL version didn't assign an array type to a domain, but newer versions (v11+) do require one. The function maintains a static counter starting from `FirstNormalObjectId` and iteratively checks the pg_type catalog until it finds an unused OID.

The function uses a simple incremental search strategy, querying the database to check if each candidate OID already exists in pg_type. This ensures that no OID conflicts occur during the upgrade process.

## Parameters / Member Variables
- `fout`: Archive structure containing the database connection and dump context
- `upgrade_query`: PQExpBuffer used to construct and execute SQL queries for OID availability checking

## Dependencies
- Functions called/Symbols referenced:
  - FirstNormalObjectId (constant defining the start of user-assignable OIDs)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formats SQL query into buffer)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (executes query expecting single row result)
  - [PQgetvalue](../P/PQgetvalue.md) (extracts result value from query)
  - [PQclear](../P/PQclear.md) (frees query result memory)
- Called from (representative examples):
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md) (src/bin/pg_dump/pg_dump.c:5406, 5441, 5442)

## Notes and Other Information
- Uses static local variable `next_possible_free_oid` to maintain state across function calls, ensuring the same OID isn't returned multiple times
- Currently only needed for domain type upgrades when migrating from pre-v11 to v11+ PostgreSQL versions
- The function performs database queries in a loop until a free OID is found, which could theoretically be expensive but is typically very fast
- Starts searching from `FirstNormalObjectId` to avoid conflicts with built-in PostgreSQL types
- The comment acknowledges that the local static state is "kind of ugly" but necessary for correct operation
- Part of the binary upgrade infrastructure that preserves object OIDs during major version upgrades