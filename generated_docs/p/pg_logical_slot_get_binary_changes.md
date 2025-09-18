# pg_logical_slot_get_binary_changes

## Location
[src/backend/replication/logical/logicalfuncs.c:349-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L349-L357)

## Overview
Returns binary changes from a logical replication slot, consuming the changes so they will not be returned again on subsequent calls.

## Definition
```sql
CREATE OR REPLACE FUNCTION pg_logical_slot_get_binary_changes(
    IN slot_name name, 
    IN upto_lsn pg_lsn, 
    IN upto_nchanges int, 
    VARIADIC options text[] DEFAULT {},
    OUT lsn pg_lsn, 
    OUT xid xid, 
    OUT data bytea
) RETURNS SETOF RECORD
```

```c
Datum pg_logical_slot_get_binary_changes(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL function provides access to logical replication changes in binary format, consuming the changes from the specified logical replication slot. Unlike the peek variant, this function advances the slots confirmed_flush position, meaning the returned changes will not be available in subsequent calls. The function is implemented as a thin wrapper around `pg_logical_slot_get_changes_guts()` with binary output enabled and confirmation set to true.

The function decodes WAL records starting from the slots restart_lsn and returns them as binary data. Each returned row contains the LSN where the change occurred, the transaction ID, and the binary-encoded change data as produced by the configured output plugin.

## Parameters / Member Variables
- `slot_name`: Name of the logical replication slot to read from
- `upto_lsn`: Optional LSN limit - function stops when this LSN is reached (NULL for no limit)
- `upto_nchanges`: Optional limit on number of changes to return (NULL for no limit) 
- `options`: Array of key-value option pairs passed to the output plugin
- `lsn` (OUT): LSN position where each change occurred
- `xid` (OUT): Transaction ID that made each change
- `data` (OUT): Binary-encoded change data from the output plugin

## Dependencies
- Functions called/Symbols referenced:
  - [pg_logical_slot_get_changes_guts](pg_logical_slot_get_changes_guts.md)
- Called from (representative examples):
  - Direct SQL function calls from applications
  - Logical replication consumers

## Notes and Other Information
- This function consumes changes, advancing the slots position
- Requires appropriate permissions to access replication slots
- The binary format depends on the configured output plugin
- Used primarily for high-performance logical replication scenarios where binary format is preferred
- Complementary to pg_logical_slot_peek_binary_changes which does not consume changes
- Defined in src/backend/replication/logical/logicalfuncs.c:349-357