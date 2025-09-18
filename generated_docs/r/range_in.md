# range_in

## Location
src/backend/utils/adt/rangetypes.c: 88 - 136

## Overview
The range_in function is the text input function for PostgreSQL range types, responsible for parsing string representations of ranges and converting them into internal RangeType structures.

## Definition


## Detailed Description
This function parses a textual representation of a range value and converts it to PostgreSQL's internal RangeType format. The function handles various range formats including empty ranges, infinite bounds, and inclusive/exclusive bounds. It performs comprehensive validation and uses the element type's input function to parse the boundary values. The function supports error contexts for proper error reporting and handles recursive parsing when the subtype is itself a range type.

## Parameters / Member Variables
-  (PG_GETARG_CSTRING(0)): The string representation of the range to be parsed
-  (PG_GETARG_OID(1)): The OID of the range type being processed
-  (PG_GETARG_INT32(2)): Type modifier for the range type
-  (fcinfo->context): Error context node for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - get_range_io_data: Retrieves I/O function cache data for the range type
  - range_parse: Parses the string representation into components
  - InputFunctionCallSafe: Safely calls the element type's input function
  - make_range: Constructs and canonicalizes the final RangeType structure
- Data structures used:
  - RangeIOData: Cache structure for I/O functions
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