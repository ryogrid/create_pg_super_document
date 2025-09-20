# equalRSDesc

## Location
[src/backend/utils/cache/relcache.c:999-1039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L999-L1039)

## Overview
Determines whether two RowSecurityDesc structures are equivalent by comparing the lists of row security policies they contain.

## Definition

```c
static bool
equalRSDesc(RowSecurityDesc *rsdesc1, RowSecurityDesc *rsdesc2)
```
## Detailed Description
This function compares two row security descriptor structures to determine if they contain equivalent sets of row security policies. It handles null pointer cases and compares the policy lists by iterating through them in parallel, using the equalPolicy function to compare individual policies.

The function assumes that RelationBuildRowSecurity builds policies in a consistent order, allowing direct parallel iteration through the policy lists without needing to sort or search.

## Parameters / Member Variables
- : First RowSecurityDesc structure to compare (may be NULL)
- : Second RowSecurityDesc structure to compare (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [RowSecurityDesc](../R/RowSecurityDesc.md) (structure type)
  - [RowSecurityPolicy](../R/RowSecurityPolicy.md) (structure type)
  - list_length (function to get list length)
  - forboth (macro for parallel list iteration)
  - lfirst (macro to get list cell value)
  - [equalPolicy](equalPolicy.md) (function to compare policies)
- Called from (representative examples):
  - [RelationClearRelation](../R/RelationClearRelation.md)

## Notes and Other Information
- Returns true if both descriptors are NULL (equivalent empty state)
- Returns false if only one descriptor is NULL
- First checks if policy list lengths are equal before detailed comparison
- Relies on consistent policy ordering from RelationBuildRowSecurity
- Uses forboth macro for efficient parallel iteration through both policy lists
- Part of PostgreSQL's Row Level Security (RLS) system infrastructure
- Used in relation cache management to determine if cached row security information needs to be updated