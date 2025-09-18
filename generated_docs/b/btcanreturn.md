# btcanreturn

## Location
src/backend/access/nbtree/nbtree.c: 1457 - 1460

## Overview
Determines whether B-tree indexes support index-only scans for a given attribute, always returning true since B-tree indexes fully support this optimization.

## Definition
```c
bool btcanreturn(Relation index, int attno)
```

## Detailed Description
The `btcanreturn` function is a simple predicate function that indicates whether B-tree indexes can support index-only scans. Index-only scans are a query optimization where the query executor can satisfy queries entirely from index data without accessing the heap table, provided the index contains all required column values.

B-tree indexes in PostgreSQL always support index-only scans because they store the actual data values (not just hash codes or partial representations) for all indexed attributes. This function therefore unconditionally returns true, making it a trivial implementation that serves as part of the index access method interface.

## Parameters / Member Variables
- `index`: The Relation representing the B-tree index being queried
- `attno`: The attribute number (column position) being checked for index-only scan support

## Dependencies
- Functions called/Symbols referenced:
  - None (trivial implementation)
- Called from:
  - [bthandler](bthandler.md) (B-tree index access method handler registration)
  - Various planner and executor components that determine scan strategies

## Notes and Other Information
- Part of the index access method (AM) interface that allows different index types to advertise their capabilities
- The trivial implementation reflects B-tree's full support for index-only scans on all attributes
- Contrasts with other index types (like hash indexes) that may have limitations on index-only scan support
- Critical for query optimization decisions in the PostgreSQL planner
- The function signature follows the standard index AM canreturn interface pattern