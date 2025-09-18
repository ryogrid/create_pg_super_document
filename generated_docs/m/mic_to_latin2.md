# mic_to_latin2

## Location
[src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:99-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c#L99-L114)

## Overview
A PostgreSQL character encoding conversion function that converts text from PostgreSQL's internal Mule Internal Code (MIC) encoding to ISO 8859-2 (Latin-2) encoding.

## Definition
```c
Datum mic_to_latin2(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function interface for converting character data from the internal Mule Internal Code (MIC) encoding to ISO 8859-2 (Latin-2) encoding. It acts as a wrapper around the `mic2latin` utility function, extracting conversion parameters from PostgreSQL function arguments and handling the conversion process. The function validates that the conversion is between the expected encodings (PG_MULE_INTERNAL to PG_LATIN2) and returns the number of bytes processed during the conversion.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument interface:
- Argument 2: `src` - Source string in MIC encoding to be converted
- Argument 3: `dest` - Destination buffer for the converted Latin-2 output
- Argument 4: `len` - Length of the source string in bytes
- Argument 5: `noError` - Boolean flag indicating whether to suppress errors on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [mic2latin](mic2latin.md) (actual conversion implementation)
  - PG_RETURN_INT32 (macro for returning integer values)
- Constants referenced:
  - PG_MULE_INTERNAL (MIC encoding identifier)
  - PG_LATIN2 (Latin-2 encoding identifier)
  - LC_ISO8859_2 (Mule character set ID for ISO 8859-2)

## Notes and Other Information
- This function is typically registered as a conversion function in PostgreSQL's encoding conversion system
- The actual conversion logic is implemented in the `mic2latin` utility function from `src/backend/utils/mb/conv.c`
- Located in: `src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:99-114`
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Returns the number of input bytes successfully converted
- The `mic2latin` function handles multibyte character validation and proper character mapping from MIC to Latin-2