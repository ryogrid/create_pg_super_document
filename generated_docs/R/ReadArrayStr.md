# ReadArrayStr

## Location
[src/backend/utils/adt/arrayfuncs.c:579-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L579-L795)

## Overview
Parses array string content enclosed in braces and converts element values to internal format, determining or validating array dimensions during parsing.

## Definition

```c
static bool
ReadArrayStr(char **srcptr,
			 FmgrInfo *inputproc,
			 Oid typioparam,
			 int32 typmod,
			 char typdelim,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 int *ndim_p,
			 int *dim,
			 int *nitems_p,
			 Datum **values_p,
			 bool **nulls_p,
			 const char *origStr,
			 Node *escontext)
```
## Detailed Description
ReadArrayStr is the core parsing function that processes the content within array braces "{ ... }" and converts element values to their internal PostgreSQL representation. It handles both dimension discovery and validation while parsing nested array structures.

The function operates as a state machine that tracks nesting levels, element counts, and delimiter expectations. It can work in two modes:
1. **Dimension discovery mode**: When ndim_p is 0, it determines dimensions from array structure
2. **Dimension validation mode**: When dimensions are pre-specified, it validates the structure matches

Key parsing behaviors:
- Uses ReadArrayToken to tokenize the input stream
- Maintains element counting at each nesting level to validate consistent dimensions
- Dynamically resizes value and null arrays as needed
- Calls element type input functions to convert string values to Datums
- Enforces array size limits (MaxArraySize) to prevent excessive memory usage
- Validates that multi-dimensional arrays have consistent sub-array lengths

The function handles NULL values explicitly through ATOK_ELEM_NULL tokens and maintains separate arrays for values and null indicators.

## Parameters / Member Variables
- : Pointer to current position in input string, advanced during parsing
- : FmgrInfo for element type's input conversion function
- : Additional parameter for element input function
- : Type modifier for element type
- : Delimiter character for array elements (type-specific)
- : Storage length of element type
- : Whether element type is passed by value
- : Alignment requirement for element type
- : Input/output parameter for number of dimensions
- : Input/output array for dimension sizes
- : Output parameter for total number of elements parsed
- : Output parameter for array of parsed element values
- : Output parameter for array of null indicators
- : Original input string (used only for error messages)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ReadArrayToken](ReadArrayToken.md)
  - palloc_array
  - repalloc_array
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - [initStringInfo](../i/initStringInfo.md)
  - MaxArraySize
  - MAXDIM
  - ArrayToken types (ATOK_LEVEL_START, ATOK_LEVEL_END, etc.)
- Called from (representative examples):
  - [array_in](../a/array_in.md)

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Expects srcptr to point to opening '{' and advances it past closing '}'
- Maintains strict validation of array structure and consistent dimensions
- Uses dynamic memory allocation and reallocation for optimal performance
- Implements comprehensive error checking with detailed error messages
- Handles arrays up to MAXDIM dimensions
- Supports both explicit NULL values and empty string elements
- Freezes dimensionality once first element is encountered to ensure consistency