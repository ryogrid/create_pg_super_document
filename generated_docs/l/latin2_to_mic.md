# latin2_to_mic

## Location
src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c: 83 - 98

## Overview
A PostgreSQL character encoding conversion function that converts text from ISO 8859-2 (Latin-2) encoding to PostgreSQL's internal Mule Internal Code (MIC) encoding.

## Definition
```c
Datum latin2_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function interface for converting character data from ISO 8859-2 (Latin-2) encoding to the internal Mule Internal Code (MIC) encoding. It extracts the conversion parameters from PostgreSQL function arguments and delegates the actual conversion work to the `latin2mic` utility function. The function validates that the conversion is between the expected encodings (PG_LATIN2 to PG_MULE_INTERNAL) and returns the number of bytes processed during the conversion.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument interface:
- Argument 2: `src` - Source string in Latin-2 encoding to be converted
- Argument 3: `dest` - Destination buffer for the converted MIC-encoded output
- Argument 4: `len` - Length of the source string in bytes
- Argument 5: `noError` - Boolean flag indicating whether to suppress errors on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [latin2mic](latin2mic.md) (actual conversion implementation)
  - PG_RETURN_INT32 (macro for returning integer values)
- Constants referenced:
  - PG_LATIN2 (Latin-2 encoding identifier)
  - PG_MULE_INTERNAL (MIC encoding identifier)
  - LC_ISO8859_2 (Mule character set ID for ISO 8859-2)

## Notes and Other Information
- This function is typically registered as a conversion function in PostgreSQL's encoding conversion system
- The actual conversion logic is implemented in the `latin2mic` utility function from `src/backend/utils/mb/conv.c`
- Located in: `src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:83-98`
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Returns the number of input bytes successfully converted