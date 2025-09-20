# cluster

## Location
[src/backend/commands/cluster.c:108-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L108-L265)

## Overview
The cluster function is the main entry point for the PostgreSQL CLUSTER command, which reorganizes tables according to the physical order of their clustered index for improved performance.

## Definition

```c
void
cluster(ParseState *pstate, ClusterStmt *stmt, bool isTopLevel)
```
## Detailed Description
The cluster function implements the CLUSTER SQL command that physically reorders heap tuples in a table to match the order of a specified index. This operation can improve query performance by reducing disk I/O when accessing data in index order. The function supports both single-table and multi-table clustering operations.

The implementation handles multiple scenarios:
1. **Single relation clustering**: When a specific table is provided, it clusters that table using either a specified index or the previously clustered index
2. **Multi-table clustering**: When no specific table is given, it clusters all tables that have a clustered index
3. **Partitioned table clustering**: For partitioned tables, it processes all partitions

To avoid deadlocks during multi-table operations, each relation is processed in a separate transaction. This requires careful memory management using a dedicated memory context that survives across transaction boundaries.

## Parameters / Member Variables
- : Parse state containing parsing context and error reporting information
- : ClusterStmt structure containing the parsed CLUSTER command details including table name, index name, and options
- : Boolean indicating whether this is a top-level command (affects transaction block restrictions)

## Dependencies
- Functions called/Symbols referenced:
  - [defGetBoolean](../d/defGetBoolean.md)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - table_open/table_close
  - [get_index_isclustered](../g/get_index_isclustered.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - [cluster_rel](cluster_rel.md)
  - [cluster_multiple_rels](cluster_multiple_rels.md)
  - [check_index_is_clusterable](check_index_is_clusterable.md)
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - AllocSetContextCreate/MemoryContextDelete
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- The function prevents execution within transaction blocks for multi-table operations to avoid holding exclusive locks on multiple tables simultaneously
- Supports a VERBOSE option for detailed output during clustering
- Automatically finds the clustered index if no index is specified for single-table operations
- Rejects clustering of remote temporary tables due to buffer manager limitations
- Uses separate memory contexts to manage data across multiple transactions in multi-table scenarios