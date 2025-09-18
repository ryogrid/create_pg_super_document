# range_out

## Location
src/backend/utils/adt/rangetypes.c: 137 - 176

## Overview
The range_out function is the text output function for PostgreSQL range types, responsible for converting internal RangeType structures into their string representations.

## Definition


## Detailed Description
This function converts a RangeType value from its internal binary representation to a human-readable string format. It deserializes the range structure to extract boundary information and flags, then calls the element type's output function to format the boundary values. Finally, it constructs the complete string representation using appropriate delimiters and brackets to indicate inclusivity/exclusivity and infinite bounds.

## Parameters / Member Variables
-  (PG_GETARG_RANGE_P(0)): The input RangeType structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
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