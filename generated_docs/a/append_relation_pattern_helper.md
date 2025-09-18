# append_relation_pattern_helper

## Location
src/bin/pg_amcheck/pg_amcheck.c: 1428 - 1477

## Overview
Core helper function that processes relation name patterns (tables, indexes) with full qualification support and type-specific filtering for the pg_amcheck utility.

## Definition
```c
static void append_relation_pattern_helper(PatternInfoArray *pia, const char *pattern, int encoding, bool heap_only, bool btree_only)
```

## Detailed Description
The `append_relation_pattern_helper` function is the core implementation for processing relation patterns in pg_amcheck. It handles fully qualified relation names in the format "database.schema.relation" and converts them into SQL regular expressions for pattern matching. The function supports filtering by relation type through boolean flags, allowing patterns to be restricted to heap tables only or btree indexes only. It validates that patterns contain at most 2 dots (fully qualified format) and will terminate the program if more complex patterns are provided. The function manages three separate buffer structures to handle database, schema, and relation components independently, setting appropriate flags and storing regex patterns for later use during database scanning.

## Parameters / Member Variables
- `pia`: PatternInfoArray pointer to the pattern information array that will be extended with the new pattern
- `pattern`: const char pointer to the relation name pattern string, potentially fully qualified
- `encoding`: int value representing the client encoding used for parsing the pattern
- `heap_only`: bool flag indicating the pattern should only match heap tables (not indexes)
- `btree_only`: bool flag indicating the pattern should only match btree indexes (not other relation types)

## Dependencies
- Functions called/Symbols referenced:
  - extend_pattern_info_array
  - initPQExpBuffer
  - patternToSQLRegex
  - pg_log_error
  - exit
  - pstrdup
  - termPQExpBuffer
- Types used:
  - PatternInfoArray
  - PQExpBufferData
  - PatternInfo
- Global variables accessed:
  - opts.dbpattern
- Called from (representative examples):
  - append_relation_pattern (at src/bin/pg_amcheck/pg_amcheck.c:1480)
  - append_heap_pattern (at src/bin/pg_amcheck/pg_amcheck.c:1496)
  - append_btree_pattern (at src/bin/pg_amcheck/pg_amcheck.c:1512)

## Notes and Other Information
- Central helper function used by multiple pattern append functions for different relation types
- Supports fully qualified patterns in "database.schema.relation" format (maximum 2 dots)
- Uses three separate PQExpBuffer structures for database, schema, and relation components
- Sets the global dbpattern flag when a database qualifier is present
- Type filtering flags (heap_only, btree_only) are stored in the PatternInfo structure for later use
- Program will exit with code 2 if patterns contain too many dotted components
- Proper memory management through buffer initialization and termination
- Forms the foundation for relation pattern matching in pg_amcheck's integrity checking operations