# heapam_relation_toast_am

## Location
src/backend/access/heap/heapam_handler.c: 2088 - 2098

## Overview
Returns the access method OID to use for a relations TOAST table, which for heap relations is the same as the parent relations access method.

## Definition
```c
static Oid heapam_relation_toast_am(Relation rel)
```

## Detailed Description
This function provides the access method OID that should be used when creating a TOAST table for the given relation. For heap relations, the TOAST table uses the same access method as the parent relation. This ensures consistency in storage and access patterns between the main relation and its associated TOAST storage.

The function simply returns the relations access method OID (rd_rel->relam) from the relation descriptor. This straightforward approach reflects that heap TOAST tables are implemented as regular heap relations, maintaining the same storage characteristics and behavior as their parent tables.

## Parameters / Member Variables
- `rel`: The relation for which to determine the TOAST table access method

## Dependencies
- Functions called/Symbols referenced:
  - None (direct struct field access only)
- Called from (representative examples):
  - SampleHeapTupleVisible

## Notes and Other Information
This function embodies a key design principle of PostgreSQLs TOAST system: TOAST tables for heap relations are themselves heap relations. This design choice simplifies the implementation and ensures that TOAST tables inherit all the reliability, concurrency, and performance characteristics of regular heap storage. Other access methods might implement different strategies for their TOAST storage, but for heap AM, the choice is straightforward and consistent.