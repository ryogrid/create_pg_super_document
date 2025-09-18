# mic_to_big5

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:129-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L129-L148)

## Overview
A PostgreSQL function that converts text from Mule Internal Code (MIC) encoding to Big5 encoding, serving as a wrapper function for the core mic2big5 conversion routine.

## Definition
```c
Datum mic_to_big5(PG_FUNCTION_ARGS)
```

## Detailed Description
The `mic_to_big5` function is a PostgreSQL conversion function that handles the conversion of character data from Mule Internal Code (MIC) encoding to Big5 encoding. It follows the standard PostgreSQL function calling convention for encoding conversion functions, extracting arguments from the function call context and delegating the actual conversion work to the `mic2big5` helper function. The function includes proper encoding validation and error handling as required by PostgreSQL's conversion framework.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `src`: Source string in MIC encoding (argument 2)
  - `dest`: Destination buffer for Big5 encoded output (argument 3) 
  - `len`: Length of the source string in bytes (argument 4)
  - `noError`: Boolean flag indicating whether to suppress errors during conversion (argument 5)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`: Extract string arguments from function call
  - `PG_GETARG_INT32`: Extract integer argument (length)
  - `PG_GETARG_BOOL`: Extract boolean argument (error handling flag)
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validate conversion between PG_MULE_INTERNAL and PG_BIG5
  - [mic2big5](mic2big5.md): Core conversion function that performs the actual encoding transformation
  - `PG_RETURN_INT32`: Return the number of converted bytes
- Called from:
  - PostgreSQL encoding conversion system (no direct references found in codebase)

## Notes and Other Information
- This function is part of PostgreSQL's encoding conversion infrastructure
- Located in the EUC_TW and Big5 conversion module
- Returns the number of bytes successfully converted
- Validates that the conversion is between the expected encodings (MIC to Big5)
- The actual conversion logic is implemented in the `mic2big5` helper function