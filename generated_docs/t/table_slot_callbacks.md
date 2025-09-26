# table_slot_callbacks

## Location
[src/backend/access/table/tableam.c:59-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L59-L91)

## Overview
Returns the appropriate TupleTableSlotOps callback structure for a given relation, determining the correct slot operations based on the relation's table access method and relation kind.

## Definition

```c
const TupleTableSlotOps *
table_slot_callbacks(Relation relation)
```
## Detailed Description
This function serves as a central dispatcher for determining the appropriate tuple table slot operations for different types of relations in PostgreSQL. It implements a three-tier decision process:

1. **Table Access Method**: If the relation has a registered table access method (rd_tableam), it delegates to that method's slot_callbacks function
2. **Foreign Tables**: For foreign tables (RELKIND_FOREIGN_TABLE), it returns heap tuple slot operations (TTSOpsHeapTuple) to maintain backward compatibility with FDWs
3. **Views and Partitioned Tables**: For views and partitioned tables, it returns virtual slot operations (TTSOpsVirtual) as these relation types don't store actual tuples

The function centralizes the knowledge of which slot type is appropriate for each relation kind, making it easier for other parts of the system (like COPY) to create appropriate slots.

## Parameters / Member Variables
- : The Relation for which to determine the appropriate slot callbacks

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md) (structure type)
  - TTSOpsHeapTuple (global variable)
  - TTSOpsVirtual (global variable)
  - RELKIND_FOREIGN_TABLE (constant)
  - RELKIND_VIEW (constant)
  - RELKIND_PARTITIONED_TABLE (constant)

- Called from (representative examples):
  - [table_slot_create](table_slot_create.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ExecGetTriggerOldSlot](../E/ExecGetTriggerOldSlot.md)
  - [ExecGetTriggerNewSlot](../E/ExecGetTriggerNewSlot.md)
  - [ExecInitSeqScan](../E/ExecInitSeqScan.md)
  - [ExecInitIndexScan](../E/ExecInitIndexScan.md)

## Notes and Other Information
- The function maintains backward compatibility for FDWs by providing heap tuple slots even though virtual slots might be more efficient
- The Assert statement ensures that only supported relation kinds (VIEW, PARTITIONED_TABLE) reach the final branch
- This function is part of the table access method (tableam) abstraction layer introduced to support pluggable storage engines
- The choice of slot type affects performance and memory usage patterns for tuple operations