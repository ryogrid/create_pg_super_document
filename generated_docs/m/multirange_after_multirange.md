# multirange_after_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2389-2401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2389-L2401)

## Overview
Determines whether the first multirange is completely after the second multirange by checking if all ranges in the first multirange come after all ranges in the second multirange.

## Definition

```c
Datum
multirange_after_multirange(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the ">>" (after) operator for multirange types. It determines whether one multirange is entirely after another multirange. The function leverages the internal  function by swapping the order of the arguments, since "mr1 after mr2" is equivalent to "mr2 before mr1".

The function retrieves the appropriate type cache for the multirange type to access comparison functions and other metadata needed for the comparison operation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : First multirange (left operand of the ">>" operator)
  - : Second multirange (right operand of the ">>" operator)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts multirange arguments from function call
  - : Multirange type structure
  - : Retrieves type cache for multirange operations
  - : Gets the OID of the multirange type
  - : Internal function to check if one multirange is before another
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- This function implements the logical inverse of the "before" operation by swapping arguments
- Returns a boolean value indicating whether the first multirange is after the second
- The function is designed to work with PostgreSQL's function call interface using the standard PG_FUNCTION_ARGS mechanism
- Performance depends on the underlying  implementation

## Simplified Source

```c
Datum
multirange_after_multirange(PG_FUNCTION_ARGS)
{
    // Extract arguments: two multiranges to compare
    MultirangeType *multirange1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *multirange2 = PG_GETARG_MULTIRANGE_P(1);

    // Get type information for the multirange
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(multirange1));

    // Use symmetric logic: mr1 >> mr2 ≡ mr2 << mr1
    PG_RETURN_BOOL(multirange_before_multirange_internal(typcache->rngtype, multirange2, multirange1));
}
```