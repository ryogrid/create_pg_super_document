# expand_vacuum_rel

## Location
[src/backend/commands/vacuum.c:881-1020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L881-L1020)

## Overview
Expands a VacuumRelation by filling in the table OID if not specified and optionally adding VacuumRelations for all partitions of a partitioned table.

## Definition

```c
static List *
expand_vacuum_rel(VacuumRelation *vrel, MemoryContext vac_context,
				  int options)
```
## Detailed Description
The expand_vacuum_rel function processes a VacuumRelation to create a complete list of relations to vacuum. If the VacuumRelation contains an OID, it simply returns a list containing that relation. If no OID is provided, it resolves the relation name to an OID and checks if it's a partitioned table. For partitioned tables, it creates additional VacuumRelation entries for each partition.

The function is designed to handle permission checks and locking carefully. It takes a transient AccessShareLock for syscache lookups and uses find_all_inheritors to discover partitions, but releases locks to avoid deadlock risks in multi-transaction scenarios. Autovacuum workers are not expected to reach this code since they supply OIDs directly.

## Parameters / Member Variables
- : The input VacuumRelation containing either an OID or a relation name to be expanded
- : Memory context in which to allocate new VacuumRelation structures
- : Vacuum options flags that control behavior (e.g., VACOPT_SKIP_LOCKED, VACOPT_VACUUM)

## Dependencies
- Functions called/Symbols referenced:
  - AmAutoVacuumWorkerProcess
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - vacuum_is_permitted_for_relation
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
- Called from (representative examples):
  - vacuum (src/backend/commands/vacuum.c:539)

## Notes and Other Information
- Only processes relations when OID is not already provided (autovacuum supplies OIDs)
- Uses transient locking strategy to minimize deadlock risk
- Handles both regular relations and partitioned tables with automatic partition discovery
- Includes permission checking via vacuum_is_permitted_for_relation
- Memory allocation is performed in the provided vac_context
- Location: src/backend/commands/vacuum.c:881-1020