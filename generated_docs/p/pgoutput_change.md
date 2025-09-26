# pgoutput_change

## Location
[src/backend/replication/pgoutput/pgoutput.c:1429-1597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1429-L1597)

## Overview
Processes and sends decoded DML (Data Manipulation Language) operations over the wire during logical replication in both streaming and non-streaming modes.

## Definition

```c
static void
pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
```
## Detailed Description
The  function is a core component of PostgreSQL's logical replication output plugin. It handles the transmission of INSERT, UPDATE, and DELETE operations from the publisher to subscribers. The function performs several critical operations:

1. **Publishability Check**: Verifies if the relation is configured for publication
2. **Action Filtering**: Checks publication settings (pubinsert, pubupdate, pubdelete) to determine if the specific DML operation should be replicated
3. **Tuple Processing**: Handles old and new tuple data, including attribute mapping for partitioned tables
4. **Row Filtering**: Applies row-level filters that may transform operations (e.g., UPDATE to INSERT/DELETE)
5. **Transaction Management**: Sends BEGIN messages when needed and manages transaction state
6. **Schema Synchronization**: Ensures schema information is sent before data changes
7. **Data Serialization**: Converts tuples to the appropriate wire format using logical replication protocol

The function supports both regular and streaming replication modes, handling transaction IDs appropriately for each mode.

## Parameters / Member Variables
- : LogicalDecodingContext containing output plugin state and configuration
- : ReorderBufferTXN representing the current transaction being processed  
- : Relation object representing the table being modified
- : ReorderBufferChange containing the specific DML operation details including action type and tuple data

## Dependencies
- Functions called/Symbols referenced:
  - [is_publishable_relation](../i/is_publishable_relation.md)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [pgoutput_row_filter](pgoutput_row_filter.md)
  - [pgoutput_send_begin](pgoutput_send_begin.md)
  - [maybe_send_schema](../m/maybe_send_schema.md)
  - [logicalrep_write_insert](../l/logicalrep_write_insert.md)
  - [logicalrep_write_update](../l/logicalrep_write_update.md)
  - [logicalrep_write_delete](../l/logicalrep_write_delete.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (as callback registration)

## Notes and Other Information
- The function operates within a dedicated memory context that is reset after each change to prevent memory leaks
- Supports partition-wise publishing where changes to partitioned tables can be published as changes to their root table
- Handles attribute mapping for tables with different schemas between publisher and subscriber
- Implements row filtering that can potentially transform UPDATE operations into INSERT or DELETE operations
- Maintains backward compatibility by only sending BEGIN messages when actual changes will be transmitted
- Critical for logical replication performance as it's called for every DML operation being replicated