# TidStoreGetDSA

## Location
[src/backend/access/common/tidstore.c:563-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L563-L570)

## Overview
Returns the DSA (Dynamic Shared Area) where a shared TidStore resides, providing access to the underlying shared memory area.

## Definition


## Detailed Description
This function provides access to the Dynamic Shared Area (DSA) that contains a shared TidStore. The DSA is PostgreSQL's mechanism for managing shared memory that can be allocated and freed dynamically across multiple processes. This function is only valid for shared TidStores and includes an assertion to verify this precondition.

The returned DSA area can be used for managing shared memory allocation and accessing other shared structures within the same memory area.

## Parameters / Member Variables
- : The shared TidStore for which to retrieve the DSA area

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - TidStoreIsShared
- Called from (representative examples):
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - [parallel_vacuum_reset_dead_items](../p/parallel_vacuum_reset_dead_items.md)
  - [test_create](../t/test_create.md)

## Notes and Other Information
- Only valid for shared TidStores; includes Assert(TidStoreIsShared(ts)) to verify this precondition
- Used in parallel vacuum operations to coordinate shared memory management
- The DSA area enables dynamic allocation of shared memory structures across multiple processes
- Essential for multi-process vacuum operations where TidStores need to be shared between worker processes
- Returns the actual DSA area pointer stored in the TidStore structure