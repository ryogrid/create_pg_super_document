# IsToastRelation

## Location
src/backend/catalog/catalog.c: 175 - 194

## Overview
Determines whether a given relation is a TOAST (The Oversized-Attribute Storage Technique) support relation or index by checking if it belongs to a pg_toast namespace.

## Definition
```c
bool IsToastRelation(Relation relation)
```

## Detailed Description
IsToastRelation checks if a relation is part of PostgreSQL's TOAST system, which is used to store large attribute values that exceed the page size limit. Rather than performing expensive catalog lookups, this function efficiently determines TOAST relations by checking whether the relation belongs to a pg_toast namespace.

The function relies on the fact that PostgreSQL enforces restrictions against creating user relations in pg_toast namespaces or moving relations into/out of these namespaces. This makes namespace checking a reliable and fast method to identify TOAST relations.

The function will not return true for TOAST tables belonging to other sessions' temporary tables, as other mechanisms are expected to prevent access to those.

## Parameters
- `relation`: The Relation structure to check

## Dependencies
- Functions called/Symbols referenced:
  - IsToastNamespace (checks if a namespace is a TOAST namespace)
  - RelationGetNamespace (extracts the namespace OID from a relation)
- Called from (representative examples):
  - heap_insert (src/backend/access/heap/heapam.c:2163)
  - heap_abort_speculative (src/backend/access/heap/heapam.c:6166, 6250)
  - ReorderBufferProcessTXN (src/backend/replication/logical/reorderbuffer.c:2297)
  - ReorderBufferToastAppendChunk (src/backend/replication/logical/reorderbuffer.c:4854)
  - CacheInvalidateHeapTuple (src/backend/utils/cache/inval.c:1231)

## Notes and Other Information
- Does not perform any catalog accesses, making it efficient for frequent use
- Used primarily in heap operations, logical replication, and cache invalidation scenarios
- Part of PostgreSQL's TOAST system architecture for handling large attribute values
- Located in src/backend/catalog/catalog.c at lines 175-194