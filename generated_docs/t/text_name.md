# text_name

## Location
src/backend/utils/adt/varlena.c: 3359 - 3381

## Overview
Converts a PostgreSQL text data type to a Name data type, handling size limitations and character encoding properly.

## Definition
```c
Datum text_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts a variable-length text string to PostgreSQL's fixed-length Name data type, which is used for storing identifiers like table names, column names, etc. The function handles oversized input by truncating it to fit within NAMEDATALEN-1 characters, ensuring proper multibyte character boundary handling. The result is zero-padded to ensure consistent storage format for the Name type.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The input text value to convert to Name type

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro to get text argument)
  - VARSIZE_ANY_EXHDR (macro to get variable-length data size excluding header)
  - VARDATA_ANY (macro to get pointer to variable-length data)
  - Name (PostgreSQL Name data type)
  - NAMEDATALEN (constant defining maximum length for Name type)
  - pg_mbcliplen (function to clip multibyte string at character boundary)
  - palloc0 (memory allocation function that zeroes the allocated space)
  - NameStr (macro to access Name as string)
  - memcpy (standard memory copy function)
  - PG_RETURN_NAME (macro to return Name value)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Truncates input text if it exceeds NAMEDATALEN-1 characters
- Uses pg_mbcliplen to ensure truncation occurs at proper multibyte character boundaries
- Allocates zero-padded memory with palloc0 to ensure consistent Name format
- The Name type is a fixed-length type used throughout PostgreSQL for identifiers
- NAMEDATALEN is typically 64 bytes in standard PostgreSQL builds
- Part of PostgreSQL's data type conversion functions in varlena.c
- Located in src/backend/utils/adt/varlena.c:3359-3381