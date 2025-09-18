# fetch_table_list

## Location
src/backend/commands/subscriptioncmds.c: 2140 - 2247

## Overview
Retrieves the complete list of tables and their attributes from specified publications on the publisher database for logical replication setup.

## Definition


## Detailed Description
This function queries the publisher database to obtain a comprehensive list of all tables included in the specified publications. It adapts its behavior based on the PostgreSQL server version to leverage newer features and optimize performance. For PostgreSQL 16+, it uses the enhanced pg_get_publication_tables function that can handle multiple publications and automatically filters partition tables whose ancestors are already published. For older versions, it queries pg_publication_tables directly. The function also handles column list information when supported (PostgreSQL 15+), while enforcing the constraint that tables cannot have different column lists across different publications to avoid data inconsistency issues.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher database
- : List of publication names to query for table information

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_server_version
  - initStringInfo
  - [get_publications_str](../g/get_publications_str.md)
  - appendStringInfo
  - appendStringInfoString
  - appendStringInfoChar
  - [pfree](../p/pfree.md)
  - walrcv_exec
  - ereport
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - slot_getattr
  - TextDatumGetCString
  - [makeRangeVar](../m/makeRangeVar.md)
  - [list_member](../l/list_member.md)
  - lappend
  - ExecClearTuple
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [walrcv_clear_result](../w/walrcv_clear_result.md)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)

## Notes and Other Information
- Uses version-aware SQL queries to optimize performance on newer PostgreSQL versions (16+)
- Enforces column list consistency across publications to prevent data synchronization conflicts
- Returns a list of RangeVar structures representing qualified table names (schema.table)
- Automatically handles partition hierarchy filtering in PostgreSQL 16+ to avoid duplicate table entries
- Column list support is conditional based on server version capabilities (PostgreSQL 15+)
- Validates that identical tables don't have conflicting column specifications across different publications