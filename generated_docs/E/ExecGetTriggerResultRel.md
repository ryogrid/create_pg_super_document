# ExecGetTriggerResultRel

## Location
[src/backend/executor/execMain.c:1295-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1295-L1370)

## Overview
Retrieves or creates a ResultRelInfo for a trigger target relation, providing efficient reuse of relation metadata for trigger execution while supporting triggers on relations not directly involved in the main query.

## Definition

```c
ResultRelInfo *
ExecGetTriggerResultRel(EState *estate, Oid relid,
						ResultRelInfo *rootRelInfo)
```
## Detailed Description
ExecGetTriggerResultRel manages ResultRelInfo structures specifically for trigger execution contexts. It first searches existing result relations from the main query and tuple routing operations. If the target relation is not found among existing ResultRelInfo structures, it creates a new one and caches it in es_trig_target_relations for future reuse. This approach optimizes trigger performance by avoiding repeated relation opening and provides a mechanism for EXPLAIN ANALYZE to report trigger runtimes on relations not directly part of the query.

## Parameters / Member Variables
- : The execution state containing cached relation information
- : OID of the target relation for trigger execution
- : Root partition's ResultRelInfo for partitioned tables (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - RelationGetRelid
  - table_open
  - makeNode
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - lappend
- Called from (representative examples):
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (multiple locations in trigger.c)

## Notes and Other Information
- Searches three potential sources: es_opened_result_relations, es_tuple_routing_result_relations, and es_trig_target_relations
- Assumes appropriate locks are already held from when the trigger event was queued, so opens relations with NoLock
- Creates new ResultRelInfo entries in es_query_cxt memory context for proper lifetime management
- Does not initialize index information since triggers typically don't require index access
- Supports self-join scenarios where multiple ResultRelInfo entries may have the same OID
- Essential for efficient execution of cascading triggers and referential integrity constraints
- Enables performance monitoring of triggers on relations not directly involved in the main query