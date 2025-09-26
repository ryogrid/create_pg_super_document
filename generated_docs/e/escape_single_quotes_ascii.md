# escape_single_quotes_ascii

## Location
[src/port/quotes.c:33-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/quotes.c#L33-L51)

## Overview
Escapes single quotes and backslashes in ASCII strings by doubling them, specifically designed for processing PostgreSQL configuration entries and creating string literals for configuration files.

## Definition

```c
char *
escape_single_quotes_ascii(const char *src)
```
## Detailed Description
This function takes an input string and creates a new escaped version where single quotes (') and backslashes (\) are doubled to make them safe for use in PostgreSQL configuration files and string literals. The function allocates memory for a result string that is potentially twice the size of the input (plus null terminator) to accommodate the worst-case scenario where every character needs escaping.

The function is specifically designed for ASCII strings and does not consider encoding issues, making it suitable for processing postgresql.conf entries and creating string literals in pg_basebackup for recovery configuration. Since PostgreSQL configuration files treat backslashes as escape characters, both single quotes and backslashes must be doubled.

The algorithm iterates through each character of the source string, and for characters that need escaping (determined by the SQL_STR_DOUBLE macro), it first writes the character itself (as the escape) then writes it again as the actual character.

## Parameters / Member Variables
- : Input null-terminated ASCII string that needs to be escaped. Must not be NULL.

## Return Value
- Returns a malloc()ed string containing the escaped version of the input
- Returns NULL if memory allocation fails
- The caller is responsible for freeing the returned string

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for memory allocation
  - : Standard C library function to get string length
  - : Macro from src/include/c.h that determines if a character needs escaping (single quote or backslash when escape_backslash is true)

- Called from (representative examples):
  -  in src/backend/utils/misc/guc.c:4504
  -  in src/bin/initdb/initdb.c:405  
  -  in src/bin/scripts/vacuumdb.c:454
  -  in src/fe_utils/recovery_gen.c:165

## Notes and Other Information
- The function allocates memory for the worst-case scenario (len * 2 + 1) where every character might need escaping
- Memory allocation failure is handled gracefully by returning NULL
- The function is specifically designed for configuration file processing and does not handle general SQL string escaping
- The SQL_STR_DOUBLE macro is called with escape_backslash=true, meaning both single quotes and backslashes are escaped
- This is a utility function in the PostgreSQL port library (src/port/) making it available across different PostgreSQL components
- The function assumes ASCII input and does not consider multi-byte character encodings