# table_tuple_insert

## Location
[src/include/access/tableam.h:1403-1421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1403-L1421)

## Overview
This function inserts a tuple from a slot into a table using the table access method abstraction, with support for various insertion options including bulk operations, frozen tuples, and logical replication control.

## Definition
```c
static inline void
table_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
                   int options, struct BulkInsertStateData *bistate)
```

## Detailed Description
The `table_tuple_insert` function serves as the primary table access method interface for inserting tuples into tables. It provides a consistent API that delegates to the specific storage engine implementation via `rel->rd_tableam->tuple_insert`.

The function supports multiple insertion modes through option flags:

- **TABLE_INSERT_SKIP_FSM**: Allows the access method to skip free space map consultation, useful for new relations without significant free space
- **TABLE_INSERT_FROZEN**: Inserts tuples as frozen (all transactions can see them), used only during controlled scenarios like table creation within the current subtransaction
- **TABLE_INSERT_NO_LOGICAL**: Disables logical decoding for the insertion, primarily used during table rewrites when logical logging status may be transitional

The function also handles bulk insertion optimization through the BulkInsertStateData parameter, which maintains state across multiple insertions for improved performance. When using bulk insert state, `table_finish_bulk_insert()` must be called to finalize the operation.

Upon successful insertion, the slot's `tts_tid` (tuple identifier) and `tts_tableOid` (table OID) are updated to reflect the new tuple's location. However, any TOAST processing (out-of-line data storage) is not reflected in the slot's contents.

## Parameters / Member Variables
- `rel`: The relation (table) into which the tuple will be inserted
- `slot`: TupleTableSlot containing the tuple data to be inserted
- `cid`: CommandId for the current command, used for visibility and MVCC purposes
- `options`: Bitmask of insertion options (TABLE_INSERT_* flags)
- `bistate`: BulkInsertStateData structure for bulk insertion optimization (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `rel->rd_tableam->tuple_insert` (access method-specific implementation)
  - `CommandId` (type)
  - `[BulkInsertStateData](../B/BulkInsertStateData.md)` (structure type)
- Called from (representative examples):
  - `[simple_table_tuple_insert](../s/simple_table_tuple_insert.md)` (src/backend/access/table/tableam.c:279)
  - `[CopyFrom](../C/CopyFrom.md)` (src/backend/commands/copyfrom.c:1267)
  - `[intorel_receive](../i/intorel_receive.md)` (src/backend/commands/createas.c:591)
  - `[ExecInsert](../E/ExecInsert.md)` (src/backend/executor/nodeModifyTable.c:1160)
  - `[ATRewriteTable](../A/ATRewriteTable.md)` (src/backend/commands/tablecmds.c:6330)

## Notes and Other Information
- Part of the table access method abstraction layer supporting pluggable storage engines
- Critical function for all tuple insertion operations in PostgreSQL
- Options are passed through to TOAST table operations when out-of-line storage is needed
- The frozen tuple option requires careful usage as it violates normal MVCC semantics
- Bulk insert state optimization is essential for high-performance data loading operations like COPY
- The function updates slot metadata but does not modify the slot's tuple data representation
- Must be paired with `table_finish_bulk_insert()` when using bulk insert state