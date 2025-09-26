# fetch_remote_table_info

## Location
[src/backend/replication/logical/tablesync.c:820-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L820-L1140)

## Overview
Retrieves comprehensive metadata about a remote table from the publisher database during logical replication table synchronization, including schema info, column details, and row filter qualifications.

## Definition

```c
struct a unified row filter expression at
		 * all.
		 */
		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
```
## Detailed Description
The  function is a critical component of PostgreSQL's logical replication table synchronization process. It performs multiple SQL queries against the publisher database to gather complete metadata about a target table, which is essential for properly setting up replication.

The function operates in several phases:

1. **Basic Table Information**: Queries pg_class and pg_namespace to fetch the table's OID, replica identity setting, and relation kind
2. **Column List Filtering**: For PostgreSQL 15+, fetches publication-specific column lists to determine which columns should be replicated
3. **Column Metadata**: Retrieves column names, data types, and identifies key columns used for replica identity
4. **Row Filter Expressions**: For PostgreSQL 15+, collects row filter expressions from publications to determine which rows should be synchronized

The function handles version compatibility by checking the PostgreSQL server version and only querying for features available in that version. It also performs validation to ensure consistent configuration across multiple publications.

Key features include:
- Support for selective column replication (PostgreSQL 15+)
- Row-level filtering based on publication WHERE clauses (PostgreSQL 15+)
- Comprehensive error handling with detailed error messages
- Memory management for dynamically allocated structures
- Version-aware SQL query construction

## Parameters / Member Variables
- : C string containing the schema name of the remote table
- : C string containing the table name of the remote table  
- : Pointer to LogicalRepRelation structure to be populated with table metadata (output parameter)
- : Pointer to List pointer for storing row filter qualification expressions (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_exec (executes SQL queries on publisher connection)
  - walrcv_server_version (checks publisher PostgreSQL version)
  - [walrcv_clear_result](../w/walrcv_clear_result.md) (cleans up query results)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (creates tuple slots for result processing)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md) (cleans up tuple slots)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (retrieves tuples from result sets)
  - [slot_getattr](../s/slot_getattr.md) (extracts column values from tuples)
  - [quote_literal_cstr](../q/quote_literal_cstr.md) (safely quotes SQL string literals)
  - [bms_add_member](../b/bms_add_member.md), bms_is_member (bitmap set operations for column tracking)
  - LogRepWorkerWalRcvConn (global WAL receiver connection)
  - MySubscription (global subscription information)

- Called from (representative examples):
  - [copy_table](../c/copy_table.md) (uses the retrieved metadata for COPY operations during initial sync)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:820-1140
- This is a static helper function used internally within the tablesync module
- Performs extensive error checking and reports detailed error messages for connection failures or missing tables
- Handles memory allocation for column names and types arrays with MaxTupleAttributeNumber sizing
- Supports PostgreSQL version compatibility by conditionally querying features based on server version
- Implements complex logic for handling multiple publications with potentially different column lists and row filters
- Critical for ensuring data consistency between publisher and subscriber by validating schema compatibility
- The function can handle scenarios where publications have conflicting specifications and reports appropriate errors
- Row filters from multiple publications are combined using OR logic during COPY operations