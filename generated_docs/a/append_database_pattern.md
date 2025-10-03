# append_database_pattern

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1356-1385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1356-L1385)

## Overview
Adds a database name pattern to a pattern information array for use in PostgreSQL's pg_amcheck utility.

## Definition

```c
static void
append_database_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
```
## Detailed Description
The  function processes a database name pattern and adds it to the provided pattern information array. It converts the user-provided pattern into a SQL regular expression that can be used for database matching operations. The function validates that the pattern contains no qualified names (no dots) as database patterns should represent simple database names only. If the pattern contains dots, indicating an improperly qualified name, the function logs an error and exits the program.

## Parameters / Member Variables
- `*pia`: PatternInfoArray pointer to the pattern information array that will be extended with the new pattern
- `*pattern`: const char pointer to the database name pattern string to be processed
- `encoding`: int value representing the client encoding used for parsing the pattern
## Dependencies
- Functions called/Symbols referenced:
  - [extend_pattern_info_array](../e/extend_pattern_info_array.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [patternToSQLRegex](../p/patternToSQLRegex.md)
  - pg_log_error
  - exit
  - [pstrdup](../p/pstrdup.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Types used:
  - [PatternInfoArray](../P/PatternInfoArray.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [PatternInfo](../P/PatternInfo.md)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:311, 315)

## Notes and Other Information
- This function is part of the pg_amcheck utility which is used for checking PostgreSQL database integrity
- Database patterns must be simple names without dots - qualified names with schema or other qualifiers are not allowed
- The function will terminate the program (exit(2)) if an improperly qualified pattern is provided
- The converted SQL regex is stored in the db_regex field of the PatternInfo structure
- Memory management is handled through PQExpBuffer allocation and deallocation