# fmtHba

## Location
[src/test/regress/pg_regress.c:923-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L923-L948)

## Overview
Formats a string for use in PostgreSQL HBA (Host-Based Authentication) configuration by properly escaping quotes and wrapping in double quotes.

## Definition
```c
static const char *fmtHba(const char *raw)
```

## Detailed Description
The `fmtHba` function takes a raw string and formats it for safe inclusion in PostgreSQL's pg_hba.conf file. It wraps the entire input string in double quotes and escapes any existing double quote characters by doubling them (standard CSV/HBA escaping). The function uses a static buffer that is reallocated as needed to accommodate strings of varying lengths, returning a pointer to the formatted string.

This function is specifically designed to support SSPI authentication configuration, ensuring that usernames or other values containing special characters are properly quoted for the HBA configuration format.

## Parameters / Member Variables
- `raw`: The input string to be formatted for HBA configuration

## Dependencies
- Functions called/Symbols referenced:
  - [pg_realloc](../p/pg_realloc.md)
- Called from (representative examples):
  - CW (macro or function, called twice)

## Notes and Other Information
- Uses a static buffer that persists between calls and grows as needed
- Implements standard CSV-style quote escaping (" becomes "")
- Always wraps the result in double quotes regardless of content
- Part of the PostgreSQL regression testing framework's SSPI authentication support
- The returned pointer remains valid until the next call to fmtHba
- Memory efficient - reuses and grows the static buffer rather than allocating new memory each time