# koi8r_to_mic

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:307-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L307-L322)

## Overview
Converts text from KOI8-R (Cyrillic) encoding to PostgreSQL's internal MULE encoding format.

## Definition

```c
Datum
koi8r_to_mic(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from KOI8-R encoding (a popular Cyrillic character encoding used primarily in Russian computing) to MULE (Multi-Language Environment) internal encoding format. It acts as a PostgreSQL function callable from SQL and is part of the character encoding conversion infrastructure. The function delegates the actual conversion work to the generic  helper function, which handles the conversion for character sets where local codes map directly to MIC codes.

## Parameters / Member Variables
The function follows PostgreSQL's standard function argument protocol (PG_FUNCTION_ARGS), which provides:
-  (argument 2): Source string in KOI8-R encoding to be converted
-  (argument 3): Destination buffer where converted MULE-encoded string will be written
-  (argument 4): Length of the source string in bytes
-  (argument 5): Boolean flag indicating whether to suppress error reporting on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string arguments from PostgreSQL function call
  -  - Extract integer argument from PostgreSQL function call  
  -  - Extract boolean argument from PostgreSQL function call
  -  - Validate encoding conversion parameters
  -  - Perform the actual character conversion
  -  - Return integer result to PostgreSQL
- Called from:
  - PostgreSQL encoding conversion system (no direct callers found in codebase)

## Notes and Other Information
- Part of the cyrillic_and_mic conversion module located in src/backend/utils/mb/conversion_procs/
- Uses the LC_KOI8_R locale constant and PG_KOI8R encoding identifier
- Returns the number of input bytes successfully converted
- The conversion leverages the fact that KOI8-R high-bit characters can be directly mapped to MULE internal format with appropriate locale prefixes
- This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system rather than called directly