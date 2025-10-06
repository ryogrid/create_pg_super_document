# range_in

## Location
[src/backend/utils/adt/rangetypes.c:88-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L88-L136)

## Overview
The range_in function is the text input function for PostgreSQL range types, responsible for parsing string representations of ranges and converting them into internal RangeType structures.

## Definition

```c
struct result string */
	output_str = range_deparse(flags, lbound_str, ubound_str);
```
## Detailed Description
This function parses a textual representation of a range value and converts it to PostgreSQL's internal RangeType format. The function handles various range formats including empty ranges, infinite bounds, and inclusive/exclusive bounds. It performs comprehensive validation and uses the element type's input function to parse the boundary values. The function supports error contexts for proper error reporting and handles recursive parsing when the subtype is itself a range type.

## Parameters / Member Variables
-  (PG_GETARG_CSTRING(0)): The string representation of the range to be parsed
-  (PG_GETARG_OID(1)): The OID of the range type being processed
-  (PG_GETARG_INT32(2)): Type modifier for the range type
-  (fcinfo->context): Error context node for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - [get_range_io_data](../g/get_range_io_data.md): Retrieves I/O function cache data for the range type
  - [range_parse](range_parse.md): Parses the string representation into components
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md): Safely calls the element type's input function
  - [make_range](../m/make_range.md): Constructs and canonicalizes the final RangeType structure
- Data structures used:
  - [RangeIOData](../R/RangeIOData.md): Cache structure for I/O functions
  - RangeBound: Structure representing range boundaries
  - IOFunc_input: Enum value for input function type
- Macros used:
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND: Check for boundary existence
  - RANGE_LB_INF/RANGE_UB_INF: Infinite boundary flags
  - RANGE_LB_INC/RANGE_UB_INC: Inclusive boundary flags
  - RANGE_EMPTY: Empty range flag
  - PG_RETURN_RANGE_P: Return macro for range types

## Notes and Other Information
- The function includes stack depth checking to prevent stack overflow during recursive parsing of nested range types
- Uses safe input function calls with error context support for proper error handling
- Handles all range boundary combinations: finite/infinite, inclusive/exclusive
- The parsing process separates flag extraction from boundary value parsing for better error handling
- Canonicalization is performed through make_range to ensure consistent internal representation

## Simplified Source

```c
Datum
range_in(PG_FUNCTION_ARGS)
{
    char *input_str = PG_GETARG_CSTRING(0);
    Oid range_type_oid = PG_GETARG_OID(1);
    int32 typmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;

    check_stack_depth(); // Guard against recursion

    // Get I/O cache for this range type
    RangeIOData *cache = get_range_io_data(fcinfo, range_type_oid, IOFunc_input);

    // Parse string into flags and boundary strings
    char flags;
    char *lower_str, *upper_str;
    if (!range_parse(input_str, &flags, &lower_str, &upper_str, escontext))
        PG_RETURN_NULL();

    // Parse boundary values using element type's input function
    RangeBound lower, upper;
    if (RANGE_HAS_LBOUND(flags))
        if (!InputFunctionCallSafe(&cache->typioproc, lower_str,
                                   cache->typioparam, typmod,
                                   escontext, &lower.val))
            PG_RETURN_NULL();

    if (RANGE_HAS_UBOUND(flags))
        if (!InputFunctionCallSafe(&cache->typioproc, upper_str,
                                   cache->typioparam, typmod,
                                   escontext, &upper.val))
            PG_RETURN_NULL();

    // Set boundary properties from flags
    lower.infinite = (flags & RANGE_LB_INF) != 0;
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;
    upper.infinite = (flags & RANGE_UB_INF) != 0;
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    // Create and canonicalize the range
    RangeType *range = make_range(cache->typcache, &lower, &upper,
                                  flags & RANGE_EMPTY, escontext);

    PG_RETURN_RANGE_P(range);
}
```