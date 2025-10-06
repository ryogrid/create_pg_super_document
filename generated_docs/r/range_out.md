# range_out

## Location
[src/backend/utils/adt/rangetypes.c:137-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L137-L176)

## Overview
The range_out function is the text output function for PostgreSQL range types, responsible for converting internal RangeType structures into their string representations.

## Definition

```c
struct result string */
	output_str = range_deparse(flags, lbound_str, ubound_str);
```
## Detailed Description
This function converts a RangeType value from its internal binary representation to a human-readable string format. It deserializes the range structure to extract boundary information and flags, then calls the element type's output function to format the boundary values. Finally, it constructs the complete string representation using appropriate delimiters and brackets to indicate inclusivity/exclusivity and infinite bounds.

## Parameters / Member Variables
-  (PG_GETARG_RANGE_P(0)): The input RangeType structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - [get_range_io_data](../g/get_range_io_data.md): Retrieves I/O function cache data for the range type
  - RangeTypeGetOid: Extracts the OID from the range type
  - [range_deserialize](range_deserialize.md): Extracts boundary information from the range
  - [range_get_flags](range_get_flags.md): Retrieves the range flags
  - [OutputFunctionCall](../O/OutputFunctionCall.md): Calls the element type's output function for boundaries
  - [range_deparse](range_deparse.md): Constructs the final string representation
- Data structures used:
  - [RangeIOData](../R/RangeIOData.md): Cache structure for I/O functions
  - RangeBound: Structure representing range boundaries
  - IOFunc_output: Enum value for output function type
- Macros used:
  - PG_GETARG_RANGE_P: Macro to extract range argument
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND: Check for boundary existence
  - PG_RETURN_CSTRING: Return macro for C-string results
- Called from (representative examples):
  - [anyrange_out](../a/anyrange_out.md): Output function for anyrange pseudotype
  - [anycompatiblerange_out](../a/anycompatiblerange_out.md): Output function for anycompatiblerange pseudotype

## Notes and Other Information
- The function includes stack depth checking to prevent stack overflow during recursive formatting of nested range types
- Only formats boundary values that actually exist (not infinite bounds)
- The actual string formatting and delimiter selection is delegated to range_deparse
- Handles all range types uniformly through the type cache system
- Memory management for the output string is handled by the range_deparse function

## Simplified Source

```c
Datum
range_out(PG_FUNCTION_ARGS)
{
    RangeType *range = PG_GETARG_RANGE_P(0);

    check_stack_depth(); // Guard against recursion

    // Get I/O cache for this range type
    RangeIOData *cache = get_range_io_data(fcinfo, RangeTypeGetOid(range), IOFunc_output);

    // Extract range components
    RangeBound lower, upper;
    bool empty;
    range_deserialize(cache->typcache, range, &lower, &upper, &empty);
    char flags = range_get_flags(range);

    // Convert boundary values to strings using element type's output function
    char *lower_str = NULL;
    char *upper_str = NULL;
    if (RANGE_HAS_LBOUND(flags))
        lower_str = OutputFunctionCall(&cache->typioproc, lower.val);
    if (RANGE_HAS_UBOUND(flags))
        upper_str = OutputFunctionCall(&cache->typioproc, upper.val);

    // Format the complete range string
    char *output_str = range_deparse(flags, lower_str, upper_str);

    PG_RETURN_CSTRING(output_str);
}
```