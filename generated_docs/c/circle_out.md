# circle_out

## Location
[src/backend/utils/adt/geo_ops.c:4681-4702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4681-L4702)

## Overview
Converts PostgreSQL's internal CIRCLE data structure to its external string representation in the standard format.

## Definition

```c
Datum
circle_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The `circle_out` function is the output conversion routine for PostgreSQL's CIRCLE geometric type. It takes a CIRCLE structure from the internal binary format and converts it to a standardized string representation that can be displayed to users or stored as text. The function generates the standard format `"<(x,y),radius>"` where (x,y) represents the center coordinates and radius is the circle's radius.

The function uses PostgreSQL's StringInfo mechanism to efficiently build the output string by appending individual components in the correct order with appropriate delimiters. This ensures consistent formatting that matches the expected input format for the corresponding `circle_in` function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments, containing:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves input circle argument)
  - [initStringInfo](../i/initStringInfo.md) (initializes string buffer)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (appends single characters)
  - [pair_encode](../p/pair_encode.md) (formats center point coordinates)
  - [single_encode](../s/single_encode.md) (formats radius value)
  - PG_RETURN_CSTRING (returns the formatted string)
- Constants referenced:
  - LDELIM_C, LDELIM (left delimiter characters)
  - RDELIM, RDELIM_C (right delimiter characters)
  - DELIM (separator delimiter character)
- Types referenced:
  - CIRCLE (input geometric type)
  - [StringInfoData](../S/StringInfoData.md) (string building structure)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Produces output in the standard format: `"<(x,y),radius>"`
- The output format is complementary to the input format accepted by `circle_in`
- Uses efficient string building techniques to minimize memory allocations
- Handles all valid CIRCLE values including those with NaN coordinates or radius
- The generated string is automatically null-terminated and memory-managed by PostgreSQL
- Located in src/backend/utils/adt/geo_ops.c:4681-4702