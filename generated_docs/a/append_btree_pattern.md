# append_btree_pattern

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1510-1536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1510-L1536)

## Overview
Adds a given pattern to be matched only against btree indexes in PostgreSQL's pg_amcheck utility.

## Definition

```c
static void
append_btree_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
```
## Detailed Description
This function is a specialized wrapper around `append_relation_pattern_helper` that specifically targets btree indexes. It's part of the pg_amcheck utility's pattern matching system, which allows users to specify which database objects should be checked for corruption. The function parses the provided pattern and adds it to the pattern info array with btree-only matching enabled.

## Parameters / Member Variables
- `pia`: Pointer to PatternInfoArray structure that holds the collection of patterns to be processed
- `pattern`: C string containing the relation name pattern (can include wildcards and schema qualifiers)
- `encoding`: Client encoding identifier used for parsing the pattern correctly

## Dependencies
- Functions called/Symbols referenced:
  - [append_relation_pattern_helper](append_relation_pattern_helper.md)
  - [PatternInfoArray](../P/PatternInfoArray.md)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:325)
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:329)

## Notes and Other Information
- This is a static function within pg_amcheck.c, meaning it's only accessible within that compilation unit
- The function passes `false` for heap_only and `true` for btree_only to the helper function, ensuring the pattern only matches btree indexes
- Part of pg_amcheck's command-line interface for specifying which btree indexes to check for corruption
- The pattern parsing supports PostgreSQL's standard naming conventions including schema-qualified names