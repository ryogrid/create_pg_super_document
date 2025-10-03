# NewHeapCreateToastTable

## Location
[src/backend/catalog/toasting.c:64-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/toasting.c#L64-L70)

## Overview
NewHeapCreateToastTable is a function that creates a TOAST table for a newly created heap relation, typically used during table rebuilding operations like CLUSTER.

## Definition

```c
void
NewHeapCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
						Oid OIDOldToast)
```
## Detailed Description
This function is specialized for creating TOAST tables when building new heap relations, particularly during operations that create a completely new physical copy of a table such as CLUSTER or VACUUM FULL. Unlike AlterTableCreateToastTable, this function is designed for scenarios where a new heap is being created and needs its own TOAST table setup.

The function allows passing an old TOAST table OID, which can be useful for operations that are rebuilding tables and need to reference or coordinate with the previous TOAST table structure. It delegates the actual work to CheckAndCreateToastTable with the  parameter set to false, indicating this is for new heap creation rather than altering existing tables.

## Parameters / Member Variables
- `relOid`: The OID of the new relation for which to create a TOAST table
- `reloptions`: Datum containing reloptions for the TOAST table, or (Datum) 0 for default options
- `lockmode`: The lock mode to use when accessing the relation
- `OIDOldToast`: The OID of the old TOAST table, if any (can be InvalidOid if no old TOAST table exists)
## Dependencies
- Functions called/Symbols referenced:
  - [CheckAndCreateToastTable](../C/CheckAndCreateToastTable.md)
- Called from (representative examples):
  - [make_new_heap](../m/make_new_heap.md) (in src/backend/commands/cluster.c:795)

## Notes and Other Information
- This function is specifically designed for new heap creation scenarios, distinguishing it from ALTER TABLE operations
- The  parameter is set to false when calling CheckAndCreateToastTable, which affects the validation behavior
- The OIDOldToast parameter allows coordination with existing TOAST tables during table rebuilding operations
- Commonly used in table rebuilding operations like CLUSTER that create entirely new physical table structures

## Simplified Source

```c
void NewHeapCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode, Oid OIDOldToast)
{
    // Delegate to main TOAST creation function for new heap
    CheckAndCreateToastTable(relOid, reloptions, lockmode, false, OIDOldToast);
}
```