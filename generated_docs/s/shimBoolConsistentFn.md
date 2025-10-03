# shimBoolConsistentFn

## Location
[src/backend/access/gin/ginlogic.c:108-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginlogic.c#L108-L147)

## Overview
A shim function that implements binary logic consistency checking using a ternary logic consistent function, converting GIN_MAYBE results to true with recheck flag.

## Definition

```c
static bool
shimBoolConsistentFn(GinScanKey key)
```
## Detailed Description
This function serves as an adapter that allows binary logic consistency checking using a ternary logic consistent function provided by the operator class. It calls the ternary consistent function and interprets the three-valued result (GIN_FALSE, GIN_MAYBE, GIN_TRUE) for binary logic use. The key behavior is that GIN_MAYBE results are converted to true with the recheckCurItem flag set, indicating that heap-level rechecking is required. This allows operator classes that only provide ternary consistent functions to be used in contexts that expect binary logic.

## Parameters / Member Variables
- `key`: A GinScanKey pointer containing all the information needed for the consistency check, including the ternary function to call and result storage.
## Dependencies
- Functions called/Symbols referenced:
  - [GinScanKey](../G/GinScanKey.md) (struct type)
  - GinTernaryValue (enum type)
  - [FunctionCall7Coll](../F/FunctionCall7Coll.md) (function call interface with collation)
  - DatumGetGinTernaryValue (datum conversion to GinTernaryValue)
  - [UInt16GetDatum](../U/UInt16GetDatum.md) (datum conversion for strategy)
  - [UInt32GetDatum](../U/UInt32GetDatum.md) (datum conversion for user entries count)
  - GIN_MAYBE (enum value)
- Called from (representative examples):
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md)

## Notes and Other Information
- This is a static function, only visible within the ginlogic.c compilation unit
- Part of the GIN indexing infrastructure in PostgreSQL
- Provides backward compatibility by allowing ternary consistent functions to be used in binary logic contexts
- Key conversion logic: GIN_TRUE → true (recheckCurItem = false), GIN_FALSE → false (recheckCurItem = false), GIN_MAYBE → true (recheckCurItem = true)
- The recheckCurItem flag is set appropriately based on the ternary result to ensure correct behavior
- Uses the same 7-parameter function call as directTriConsistentFn but converts the result for binary use
- Essential for operator classes that only implement ternary consistent functions but need to work in binary contexts
- Located in src/backend/access/gin/ginlogic.c:108-147

## Simplified Source

```c
static bool
shimBoolConsistentFn(GinScanKey key)
{
    GinTernaryValue result;

    // Call ternary consistent function
    result = DatumGetGinTernaryValue(FunctionCall7Coll(key->triConsistentFmgrInfo,
                                                       key->collation,
                                                       PointerGetDatum(key->entryRes),
                                                       UInt16GetDatum(key->strategy),
                                                       key->query,
                                                       UInt32GetDatum(key->nuserentries),
                                                       PointerGetDatum(key->extra_data),
                                                       PointerGetDatum(key->queryValues),
                                                       PointerGetDatum(key->queryCategories)));

    // Convert ternary result to boolean logic
    if (result == GIN_MAYBE) {
        key->recheckCurItem = true;   // Need heap-level recheck
        return true;
    } else {
        key->recheckCurItem = false;  // No recheck needed
        return result;                // GIN_TRUE or GIN_FALSE cast to bool
    }
}
```