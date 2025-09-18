# circle_in

## Location
src/backend/utils/adt/geo_ops.c: 4611 - 4680

## Overview
Parses a string representation of a circle into PostgreSQL's internal CIRCLE data structure, supporting multiple input formats including standard and quick entry styles.

## Definition


## Detailed Description
The `circle_in` function is the input conversion routine for PostgreSQL's CIRCLE geometric type. It parses string representations of circles and converts them into the internal binary format. The function supports two main input formats:

1. Standard format: `"<(x,y),radius>"` where (x,y) is the center point and radius is the circle's radius
2. Quick entry format: `"x,y,radius"` for simpler input

The parser handles various delimiter combinations, whitespace, and parenthesis nesting. It performs comprehensive validation to ensure the input string is well-formed and that the radius is non-negative (though NaN values are accepted). The function uses a depth-tracking mechanism to properly handle nested parentheses and ensures complete consumption of the input string.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments, containing:
  - Input string (accessed via PG_GETARG_CSTRING(0))
  - Error context information for proper error reporting

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (retrieves input string argument)
  - [palloc](../p/palloc.md) (allocates memory for CIRCLE structure)
  - pair_decode (parses center point coordinates)
  - single_decode (parses radius value)
  - ereturn (error return with context support)
  - PG_RETURN_CIRCLE_P (returns the parsed circle)
- Constants referenced:
  - LDELIM_C, LDELIM (left delimiter characters)
  - RDELIM, RDELIM_C (right delimiter characters)
  - DELIM (general delimiter character)
- Types referenced:
  - CIRCLE (output geometric type)
  - [Node](../N/Node.md) (for error context)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function accepts NaN as a valid radius value for special cases
- Comprehensive input validation prevents malformed circles with negative radii
- Error messages include the original input string for better debugging
- The parser is flexible with whitespace and supports nested parentheses
- Uses PostgreSQL's error context system for proper error reporting in different calling contexts
- Implements a depth-tracking system to handle complex parenthesis nesting
- Located in src/backend/utils/adt/geo_ops.c:4611-4680