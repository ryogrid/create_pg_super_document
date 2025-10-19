# trueConsistentFn

## Location
[src/backend/access/gin/ginlogic.c:50-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginlogic.c#L50-L55)

## Overview
A dummy consistent function for GIN index EVERYTHING keys that always returns true, claiming the key matches any query.

## Definition

```c
static bool
trueConsistentFn(GinScanKey key)
```
## Detailed Description
This function serves as a placeholder consistent function for GIN (Generalized Inverted Index) scan keys that represent "EVERYTHING" - meaning they should match all possible values. It's a trivial implementation that always indicates a match without performing any actual consistency checking. The function also sets the recheckCurItem flag to false, indicating that no additional verification is needed at the heap level.

## Parameters / Member Variables
- `key`: A GinScanKey pointer representing the scan key being processed. The function modifies the recheckCurItem field of this key.
## Dependencies
- Functions called/Symbols referenced:
  - [GinScanKey](../G/GinScanKey.md) (struct type)
  - GinTernaryValue (enum type)
- Called from (representative examples):
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md)

## Notes and Other Information
- This is a static function, only visible within the ginlogic.c compilation unit
- Part of the GIN indexing infrastructure in PostgreSQL
- Used specifically for keys that should match everything, eliminating the need for complex consistency logic
- The recheckCurItem = false assignment indicates that no heap-level rechecking is required since the key matches everything by definition
- Located in src/backend/access/gin/ginlogic.c:50-55

## Simplified Source

```c
static bool trueConsistentFn(GinScanKey key) {
    // Always return true for EVERYTHING keys - no consistency check needed
    key->recheckCurItem = false;  // No heap-level recheck required
    return true;
}
```