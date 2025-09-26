# array_dim_to_json

## Location
[src/backend/utils/adt/json.c:422-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L422-L464)

## Overview
Recursively processes a single dimension of a multi-dimensional array to convert it into JSON array format with proper nesting and element conversion.

## Definition
```c
static void array_dim_to_json(StringInfo result, int dim, int ndims, int *dims, Datum *vals,
                              bool *nulls, int *valcount, JsonTypeCategory tcategory,
                              Oid outfuncoid, bool use_line_feeds)
```

## Detailed Description
array_dim_to_json is a recursive function that handles the conversion of PostgreSQL multi-dimensional arrays into JSON array format. It processes one dimension at a time, starting from the outermost dimension and working inward. For the innermost dimension, it converts individual array elements to JSON using datum_to_json_internal. For outer dimensions, it recursively calls itself to process the next inner dimension. The function constructs proper JSON array syntax with square brackets and comma separators, with optional line feeds for formatting.

## Parameters / Member Variables
- `result`: StringInfo buffer where the JSON output is accumulated
- `dim`: Current dimension being processed (0-based index)
- `ndims`: Total number of dimensions in the array
- `dims`: Array containing the size of each dimension
- `vals`: Array of Datum values containing the actual array elements
- `nulls`: Boolean array indicating which elements are NULL
- `valcount`: Pointer to counter tracking the current position in vals/nulls arrays
- `tcategory`: JsonTypeCategory indicating how to convert element types
- `outfuncoid`: Output function OID for element type conversion
- `use_line_feeds`: Boolean controlling whether to add line feeds for formatting

## Dependencies
- Functions called/Symbols referenced:
  - datum_to_json_internal (for converting individual array elements)
  - array_dim_to_json (recursive call for inner dimensions)
  - appendStringInfoChar, appendStringInfoString (for building JSON string)
  - JsonTypeCategory (enumeration type)
- Called from (representative examples):
  - array_to_json_internal
  - array_dim_to_json (recursive calls)

## Notes and Other Information
The function uses a recursive approach where each call handles one dimension level. Line feeds are only used for the outermost dimension when use_line_feeds is true - inner dimensions always use compact formatting without line feeds. The valcount parameter is incremented only when processing the innermost dimension elements, ensuring proper traversal through the flattened array values.