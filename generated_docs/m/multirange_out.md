# multirange_out

## Location
src/backend/utils/adt/multirangetypes.c: 299 - 336

## Overview
Converts a PostgreSQL multirange value to its string representation, formatting it as a curly bracket-delimited list of ranges.

## Definition


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
  - get_multirange_io_data
  - IOFunc_output
  - multirange_deserialize
  - OutputFunctionCall
  - RangeTypePGetDatum
  - PG_RETURN_CSTRING
- Called from:
  - anymultirange_out (src/backend/utils/adt/pseudotypes.c:238)
  - anycompatiblemultirange_out (src/backend/utils/adt/pseudotypes.c:251)

## Notes and Other Information
- Uses StringInfo buffer for efficient string building
- Delegates individual range formatting to the underlying range type's output function
- No whitespace is added around commas in the output format
- Works with any multirange type by using cached I/O function information
- The output format matches exactly what multirange_in expects as input