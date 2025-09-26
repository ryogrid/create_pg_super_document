# has_superclass

## Location
[src/backend/catalog/pg_inherits.c:377-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L377-L405)

## Overview
Determines whether a relation inherits from any parent relation by scanning pg_inherits for entries where the relation appears as a child.

## Definition
```c
bool has_superclass(Oid relationId)
```

## Detailed Description
This function provides an accurate check to determine if a relation inherits from any parent relation. Unlike has_subclass, this function always returns correct results since it directly scans the pg_inherits system catalog to look for entries where the specified relation appears as a child (inhrelid).

The function performs a system catalog scan using the InheritsRelidSeqnoIndexId index to efficiently find any inheritance relationships where the given relation is the child. It returns true if any such relationship is found, false otherwise.

This function requires that the caller hold a lock on the given relation to prevent concurrent modifications to the inheritance hierarchy that could affect the result.

## Parameters / Member Variables
- `relationId`: OID of the relation to check for parent relations

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (with InheritsRelationId and AccessShareLock)
  - [ScanKeyInit](../S/ScanKeyInit.md) (with Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ)
  - [systable_beginscan](../s/systable_beginscan.md) (with InheritsRelidSeqnoIndexId)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:1379)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md) (src/backend/commands/tablecmds.c:19536)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md) (src/backend/commands/trigger.c:457)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [SysScanDesc](../S/SysScanDesc.md) (scan descriptor type)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:1379)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md) (src/backend/commands/tablecmds.c:19536)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md) (src/backend/commands/trigger.c:457)

## Notes and Other Information
- Unlike has_subclass, this function is guaranteed to return accurate results
- Requires the caller to hold a lock on the relation to prevent concurrent inheritance hierarchy changes
- Uses the InheritsRelidSeqnoIndexId index for efficient scanning of pg_inherits
- Scans pg_inherits looking for the relation as a child (inhrelid), not as a parent
- Opens pg_inherits with AccessShareLock to ensure consistent reads
- Returns true immediately upon finding the first matching inheritance relationship
- This is the definitive way to check if a relation participates in inheritance as a child
- Located in src/backend/catalog/pg_inherits.c:377-405