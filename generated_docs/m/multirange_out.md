# multirange_out

## Location
[src/backend/utils/adt/multirangetypes.c:299-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L299-L336)

## Overview
Converts a PostgreSQL multirange value to its string representation, formatting it as a curly bracket-delimited list of ranges.

## Definition

```c
Datum
multirange_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the output function for PostgreSQL multirange types, responsible for converting internal multirange format into text representation. It produces output in the format  where:

- The entire multirange is bounded by curly braces 
- Individual ranges are separated by commas without spaces
- Empty multiranges are represented as 
- Each range is formatted according to the underlying range type's output function

The function deserializes the multirange into its constituent ranges, then iterates through each range calling the appropriate range output function to format individual ranges. The results are concatenated with proper comma separation and brace delimiting.

## Parameters / Member Variables
- : The multirange value to convert to string format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeTypeGetOid
  - [get_multirange_io_data](../g/get_multirange_io_data.md)
  - IOFunc_output
  - [multirange_deserialize](multirange_deserialize.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - RangeTypePGetDatum
  - PG_RETURN_CSTRING
- Called from:
  - [anymultirange_out](../a/anymultirange_out.md) (src/backend/utils/adt/pseudotypes.c:238)
  - [anycompatiblemultirange_out](../a/anycompatiblemultirange_out.md) (src/backend/utils/adt/pseudotypes.c:251)

## Notes and Other Information
- Uses StringInfo buffer for efficient string building
- Delegates individual range formatting to the underlying range type's output function
- No whitespace is added around commas in the output format
- Works with any multirange type by using cached I/O function information
- The output format matches exactly what multirange_in expects as input