# range_ne_internal

## Location
[src/backend/utils/adt/rangetypes.c:618-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L618-L624)

## Overview
This internal PostgreSQL function performs inequality comparison between two range types by negating the result of the equality comparison.

## Definition

```c
bool
range_ne_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2)
```
## Detailed Description
The  function is a simple internal implementation for range inequality comparison in PostgreSQL. Rather than implementing its own comparison logic, it leverages the existing  function and returns its logical negation. This approach ensures consistency between equality and inequality operations while minimizing code duplication. The function serves as the core logic for the "not equal" (<>) operator for range types and is used by both public operators and internal PostgreSQL indexing mechanisms.

## Parameters / Member Variables
- : Type cache entry containing comparison functions and metadata for the range's element type
- : First range value to compare (const RangeType *)
- : Second range value to compare (const RangeType *)

## Dependencies
- Functions called/Symbols referenced:
  - [range_eq_internal](range_eq_internal.md)
- Types referenced:
  - [TypeCacheEntry](../T/TypeCacheEntry.md)
  - [RangeType](../R/RangeType.md)
- Called from (representative examples):
  - [range_ne](range_ne.md)
  - RANGESTRAT_EQ (macro/function in range strategy)

## Notes and Other Information
- This function implements inequality by simply negating the result of , ensuring logical consistency
- The simplicity of this implementation reduces the chance of bugs and maintains consistency with equality semantics
- Used internally by PostgreSQL's indexing strategies and public inequality operators
- The function inherits all the type safety and comparison logic from 
- Located in src/backend/utils/adt/rangetypes.c:618-624