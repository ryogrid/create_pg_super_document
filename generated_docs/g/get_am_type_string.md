# get_am_type_string

## Location
src/backend/commands/amcmds.c: 212 - 233

## Overview
Converts a single-character access method type code into a human-readable string representation for error reporting and diagnostics.

## Definition
```c
static const char *get_am_type_string(char amtype)
```

## Detailed Description
This static utility function translates access method type character codes into descriptive string constants. It serves primarily as a helper for error reporting, converting the compact single-character representation used internally by PostgreSQL into readable text that can be displayed in error messages or logs. The function uses a simple switch statement to map known access method types to their corresponding string representations.

If an invalid or unknown access method type is provided, the function generates an error using elog(ERROR), which will terminate the current operation and report the invalid type character.

## Parameters / Member Variables
- `amtype`: A single character representing the access method type (e.g., AMTYPE_INDEX for index access methods, AMTYPE_TABLE for table access methods)

## Dependencies
- Functions called/Symbols referenced:
  - AMTYPE_INDEX
  - AMTYPE_TABLE
  - elog
  - ERROR

- Called from (representative examples):
  - [get_am_type_oid](get_am_type_oid.md) (src/backend/commands/amcmds.c:145)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (amcmds.c)
- Returns string literals ("INDEX", "TABLE") that don't need to be freed
- Will never return NULL under normal circumstances - invalid inputs cause an ERROR
- Part of PostgreSQL's access method management infrastructure
- Primarily used for generating user-friendly error messages when access method type validation fails
- The function covers the standard access method types: INDEX and TABLE