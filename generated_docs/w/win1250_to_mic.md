# win1250_to_mic

## Location
src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c: 115 - 131

## Overview
A PostgreSQL character encoding conversion function that converts text from Windows-1250 encoding to PostgreSQL's internal Mule Internal Code (MIC) encoding using a translation table.

## Definition
```c
Datum win1250_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function interface for converting character data from Windows-1250 encoding to the internal Mule Internal Code (MIC) encoding. Unlike the direct Latin-2 to MIC conversion, this function uses a translation table (`win1250_2_iso88592`) to first map Windows-1250 characters to their ISO 8859-2 equivalents, then converts them to MIC format. The function leverages the `latin2mic_with_table` utility function to perform the table-based conversion and validates that the conversion is between the expected encodings.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument interface:
- Argument 2: `src` - Source string in Windows-1250 encoding to be converted
- Argument 3: `dest` - Destination buffer for the converted MIC-encoded output
- Argument 4: `len` - Length of the source string in bytes
- Argument 5: `noError` - Boolean flag indicating whether to suppress errors on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments) 
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [latin2mic_with_table](../l/latin2mic_with_table.md) (table-based conversion implementation)
  - PG_RETURN_INT32 (macro for returning integer values)
- Constants referenced:
  - PG_WIN1250 (Windows-1250 encoding identifier)
  - PG_MULE_INTERNAL (MIC encoding identifier)
  - LC_ISO8859_2 (Mule character set ID for ISO 8859-2)
  - win1250_2_iso88592 (conversion table mapping Windows-1250 to ISO 8859-2)

## Notes and Other Information
- This function is typically registered as a conversion function in PostgreSQL's encoding conversion system
- The actual conversion logic uses the `latin2mic_with_table` utility function from `src/backend/utils/mb/conv.c`
- Located in: `src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:115-131`
- The conversion table `win1250_2_iso88592` contains mappings for characters from 0x80 to 0xFF, with 0x00 entries indicating no equivalent character
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Returns the number of input bytes successfully converted
- Windows-1250 is a Microsoft code page used primarily for Central and Eastern European languages