# append_schema_pattern

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1386-1427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1386-L1427)

## Overview
Adds a schema name pattern to a pattern information array, handling potentially qualified patterns that may include both database and schema components.

## Definition
```c
static void append_schema_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
```

## Detailed Description
The `append_schema_pattern` function processes a schema name pattern and adds it to the provided pattern information array. Unlike database patterns, schema patterns can be qualified with a database name (e.g., "database.schema"). The function converts the user-provided pattern into SQL regular expressions for both database and schema matching. It validates that the pattern contains at most one dot (database.schema format) - patterns with more than one dot are considered improperly qualified and will cause the program to exit with an error. If a database component is present in the pattern, it sets the global `opts.dbpattern` flag to indicate that database-level filtering should be applied.

## Parameters / Member Variables
- `pia`: PatternInfoArray pointer to the pattern information array that will be extended with the new pattern
- `pattern`: const char pointer to the schema name pattern string, potentially qualified with database name
- `encoding`: int value representing the client encoding used for parsing the pattern

## Dependencies
- Functions called/Symbols referenced:
  - [extend_pattern_info_array](../e/extend_pattern_info_array.md)
  - initPQExpBuffer
  - [patternToSQLRegex](../p/patternToSQLRegex.md)
  - pg_log_error
  - exit
  - [pstrdup](../p/pstrdup.md)
  - termPQExpBuffer
- Types used:
  - [PatternInfoArray](../P/PatternInfoArray.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [PatternInfo](../P/PatternInfo.md)
- Global variables accessed:
  - opts.dbpattern
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:353, 357)

## Notes and Other Information
- Part of the pg_amcheck utility for PostgreSQL database integrity checking
- Supports qualified schema patterns in the format "database.schema"
- Maximum one dot allowed in the pattern - more dots result in program termination (exit(2))
- Sets the global dbpattern flag when a database qualifier is present in the pattern
- Stores separate regex patterns for database and schema components
- Uses two separate PQExpBuffer structures to handle database and schema components independently
- Memory cleanup is properly handled through termPQExpBuffer calls