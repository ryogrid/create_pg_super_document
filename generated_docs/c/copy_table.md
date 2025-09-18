# copy_table

## Location
src/backend/replication/logical/tablesync.c: 1141 - 1292

## Overview
Performs the initial data synchronization for a table in logical replication by copying all existing data from the publisher to the subscriber using PostgreSQL's COPY protocol.

## Definition


## Detailed Description
The  function is a central component of PostgreSQL's logical replication initial table synchronization process. It orchestrates the complete process of copying existing data from a table on the publisher to the corresponding table on the subscriber.

The function operates through several coordinated phases:

1. **Remote Table Discovery**: Uses  to gather comprehensive metadata about the publisher table, including column information, data types, and any row filter expressions.

2. **Relation Mapping**: Updates the logical replication relation map and opens the local relation mapping to ensure proper attribute correspondence between publisher and subscriber tables.

3. **COPY Command Construction**: Builds an appropriate COPY command based on table characteristics:
   - For regular tables without row filters: Uses direct 
   - For views, partitioned tables, or tables with row filters: Uses  with proper WHERE clauses
   - Handles column lists to exclude generated columns and include only replicated columns
   - Combines multiple row filter expressions using OR logic

4. **Format Handling**: Supports both text and binary COPY formats, with binary format available for PostgreSQL 16+ publishers when enabled in subscription settings.

5. **Data Transfer Execution**: Initiates the COPY operation on the publisher and sets up the local COPY FROM process using  as the data source callback.

6. **Transaction Management**: Coordinates with PostgreSQL's COPY infrastructure to ensure transactional consistency during the data transfer process.

The function is designed to handle various table types (regular tables, views, partitioned tables) and supports advanced features like selective column replication and row-level filtering introduced in PostgreSQL 15.

## Parameters / Member Variables
- : Pointer to the local Relation structure representing the subscriber table that will receive the copied data. The caller is responsible for ensuring this relation is properly locked.

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md) (retrieves publisher table metadata)
  - logicalrep_relmap_update (updates relation mapping)
  - logicalrep_rel_open, logicalrep_rel_close (manages relation mapping lifecycle)
  - [make_copy_attnamelist](../m/make_copy_attnamelist.md) (creates column name list for COPY)
  - [copy_read_data](copy_read_data.md) (data source callback for COPY FROM)
  - [BeginCopyFrom](../B/BeginCopyFrom.md), CopyFrom (PostgreSQL COPY infrastructure)
  - walrcv_exec (executes COPY command on publisher)
  - [make_parsestate](../m/make_parsestate.md) (creates parser state for COPY)
  - quote_qualified_identifier, quote_identifier (SQL identifier quoting)
  - Various PostgreSQL utility functions for string manipulation and memory management

- Called from (representative examples):
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (initiates table synchronization process)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:1141-1292
- This is a static helper function used internally within the tablesync module
- Handles both simple and complex COPY scenarios based on table characteristics
- Supports version-specific features by checking publisher PostgreSQL version
- Implements comprehensive error handling for connection failures and COPY operations
- Uses a global  StringInfo structure for efficient data buffering during COPY
- Critical for maintaining data consistency during initial subscription setup
- The function assumes the local relation is already locked by the caller
- Properly cleans up resources including relation mappings and temporary structures
- Supports inheritance hierarchies by using ONLY clause for regular tables to avoid duplicating child table data
- Performance-critical function that must efficiently handle tables with millions of rows during initial sync