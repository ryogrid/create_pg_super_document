# table_slot_create

## Location
[src/backend/access/table/tableam.c:92-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L92-L112)

## Overview
Creates a TupleTableSlot for a given relation using the appropriate slot operations, optionally registering it in a resource list for cleanup.

## Definition

```c
TupleTableSlot *
table_slot_create(Relation relation, List **reglist)
```
## Detailed Description
This function creates a new TupleTableSlot specifically tailored for the given relation. It combines the relation-specific slot callback determination with slot creation in a single operation. The function:

1. Calls table_slot_callbacks() to determine the appropriate TupleTableSlotOps for the relation
2. Uses MakeSingleTupleTableSlot() to create a slot with the relation's tuple descriptor and the determined operations
3. Optionally adds the created slot to a registration list for resource management

This is a convenience function that abstracts the two-step process of determining slot callbacks and creating the slot, making it easier for callers to create appropriate slots for relations.

## Parameters / Member Variables
- : The Relation for which to create a tuple table slot
- : Optional pointer to a List pointer; if provided, the created slot will be appended to this list for resource tracking

## Dependencies
- Functions called/Symbols referenced:
  - [table_slot_callbacks](table_slot_callbacks.md) (to determine appropriate slot operations)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (to create the actual slot)
  - RelationGetDescr (macro to get relation's tuple descriptor)
  - [lappend](../l/lappend.md) (to add slot to registration list)
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md) (structure type)
  - [TupleTableSlot](../T/TupleTableSlot.md) (structure type)

- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [DoCopyTo](../D/DoCopyTo.md)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)
  - [acquire_sample_rows](../a/acquire_sample_rows.md)

## Notes and Other Information
- The reglist parameter is commonly used in executor contexts where multiple slots need to be tracked and cleaned up together
- This function combines slot callback determination and slot creation, providing a single entry point for relation-specific slot creation
- The created slot uses the relation's tuple descriptor, making it suitable for storing tuples that match the relation's schema
- This is part of the table access method abstraction, allowing different storage engines to provide their own slot implementations
- The function is widely used throughout PostgreSQL for creating slots in various contexts including COPY operations, index operations, and executor initialization