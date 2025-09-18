# gb18030_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:194-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c#L194-L214)

## Overview
PostgreSQL function that converts text from GB18030 encoding to UTF-8 encoding, serving as a character conversion entry point for the database system.

## Definition
```c
Datum gb18030_to_utf8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `gb18030_to_utf8` function is a PostgreSQL conversion function that handles the transformation of text data from GB18030 (Chinese character encoding) to UTF-8 encoding. This function serves as a public interface for the PostgreSQL character encoding conversion system and follows the standard PostgreSQL function signature pattern.

The function extracts arguments from the PostgreSQL function call context, validates the encoding conversion parameters, and delegates the actual conversion work to the `LocalToUtf` function with appropriate parameters. It uses the `gb18030_to_unicode_tree` lookup table and the `conv_18030_to_utf8` conversion callback to perform the character-by-character transformation.

The function supports both strict and non-strict conversion modes through the `noError` parameter, allowing callers to choose whether conversion failures should raise exceptions or be handled gracefully.

## Parameters / Member Variables
- `src`: Source string in GB18030 encoding (extracted from PG_FUNCTION_ARGS)
- `dest`: Destination buffer for UTF-8 output (extracted from PG_FUNCTION_ARGS) 
- `len`: Length of the source string in bytes (extracted from PG_FUNCTION_ARGS)
- `noError`: Boolean flag indicating whether to suppress conversion errors (extracted from PG_FUNCTION_ARGS)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (PostgreSQL macro)
  - PG_GETARG_INT32 (PostgreSQL macro)  
  - PG_GETARG_BOOL (PostgreSQL macro)
  - CHECK_ENCODING_CONVERSION_ARGS (PostgreSQL validation macro)
  - [LocalToUtf](../L/LocalToUtf.md) (character conversion utility)
  - conv_18030_to_utf8 (conversion callback function)
  - PG_RETURN_INT32 (PostgreSQL return macro)
  - gb18030_to_unicode_tree (lookup table)
  - PG_GB18030, PG_UTF8 (encoding constants)
- Called from (representative examples): 
  - No direct references found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
This function is registered with PostgreSQL's type conversion system and is typically invoked automatically when text data needs conversion from GB18030 to UTF-8. The function follows PostgreSQL's Version-1 calling convention using the PG_FUNCTION_ARGS macro. The return value indicates the number of bytes successfully converted, which allows callers to detect partial conversions in case of errors. The function is part of PostgreSQL's comprehensive character encoding support system that enables proper handling of international text data.