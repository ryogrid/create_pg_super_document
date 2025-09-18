# pgoutput_truncate

## Location
src/backend/replication/pgoutput/pgoutput.c: 1598 - 1665

## Overview
Processes and sends TRUNCATE operations over the wire during logical replication, handling multiple relations in a single truncate command.

## Definition


## Detailed Description
The  function handles the replication of TRUNCATE operations in PostgreSQL's logical replication system. Unlike regular DML operations that affect individual rows, TRUNCATE operations can affect multiple tables simultaneously and remove all data from specified relations.

Key functionality includes:
1. **Multi-relation Processing**: Iterates through all relations specified in the TRUNCATE command
2. **Publication Filtering**: Checks each relation for publishability and truncate publication permissions
3. **Partition Handling**: Properly handles partitioned tables based on publication configuration
4. **Cascade and Sequence Options**: Preserves TRUNCATE-specific options like CASCADE and RESTART IDENTITY
5. **Batch Processing**: Collects all valid relations and sends them in a single logical replication message
6. **Transaction Management**: Ensures proper transaction boundaries and schema synchronization

The function only sends the TRUNCATE message if at least one relation qualifies for replication.

## Parameters / Member Variables
- : LogicalDecodingContext containing output plugin state and configuration
- : ReorderBufferTXN representing the current transaction being processed
- : Number of relations involved in the TRUNCATE operation
- : Array of Relation objects representing the tables being truncated
- : ReorderBufferChange containing TRUNCATE-specific options (cascade, restart_seqs)

## Dependencies
- Functions called/Symbols referenced:
  - is_publishable_relation
  - get_rel_sync_entry
  - pgoutput_send_begin
  - maybe_send_schema
  - logicalrep_write_truncate
  - OutputPluginPrepareWrite
  - OutputPluginWrite
  - MemoryContextReset
- Called from (representative examples):
  - _PG_output_plugin_init (as callback registration)

## Notes and Other Information
- Supports both CASCADE and RESTART IDENTITY options from the original TRUNCATE command
- Properly handles partitioned tables by respecting the publish_as_relid configuration
- Only sends schema information for relations that will actually be included in the TRUNCATE message
- Uses a separate memory context that is reset after processing to prevent memory leaks
- Can handle empty TRUNCATE operations (where no relations qualify for publication) without sending unnecessary messages
- Maintains transaction consistency by ensuring BEGIN messages are sent before the TRUNCATE operation