# parsePGArray

## Location
[src/fe_utils/string_utils.c:819-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L819-L901)

## Overview
Deconstructs the text representation of a 1-dimensional PostgreSQL array into individual string items.

## Definition

```c
bool
parsePGArray(const char *atext, char ***itemarray, int *nitems)
```
## Detailed Description
This function parses PostgreSQL array literal syntax (e.g., "{item1,item2,item3}") and extracts individual elements into a dynamically allocated array of strings. The function handles both quoted and unquoted array elements, processing escape sequences within quoted strings according to PostgreSQL array literal rules.

Key parsing features:
- Expects input in the format "{item,item,item}"
- Handles quoted strings with embedded quotes and backslashes
- Processes escape sequences within quoted elements
- Allocates memory efficiently in a single block for both pointer array and string data
- Returns boolean success status with populated output parameters

The parser is designed for frontend utilities that need to process array values returned from PostgreSQL queries.

## Parameters / Member Variables
- `*atext`: Input string containing the PostgreSQL array literal to parse
- `***itemarray`: Output parameter - pointer to allocated array of string pointers (caller must free)
- `*nitems`: Output parameter - number of items found in the array
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - strlen

- Called from (representative examples):
  - [buildACLCommands](../b/buildACLCommands.md) (src/bin/pg_dump/dumputils.c:137, 144)
  - [dumpSearchPath](../d/dumpSearchPath.md) (src/bin/pg_dump/pg_dump.c:3633)
  - [getPublicationTables](../g/getPublicationTables.md) (src/bin/pg_dump/pg_dump.c:4623)
  - [dumpSubscription](../d/dumpSubscription.md) (src/bin/pg_dump/pg_dump.c:5182)
  - [dumpFunc](../d/dumpFunc.md) (src/bin/pg_dump/pg_dump.c:12498)
  - [dumpIndex](../d/dumpIndex.md) (src/bin/pg_dump/pg_dump.c:17033, 17035)
  - [processExtensionTables](processExtensionTables.md) (src/bin/pg_dump/pg_dump.c:18422, 18424)
  - [appendReloptionsArray](../a/appendReloptionsArray.md) (src/fe_utils/string_utils.c:973)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:819-901
- Returns false on parse failure (malformed input, premature string end, memory allocation failure)
- Memory management: single malloc block contains both pointer array and string data for easy cleanup
- Worst-case memory allocation accounts for maximum possible items (one per input character)
- Heavily used by pg_dump utilities for processing various PostgreSQL array types
- Handles PostgreSQL-specific array literal syntax including proper quote and escape processing