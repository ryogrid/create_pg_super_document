# append_heap_pattern

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1494-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1494-L1509)

## Overview
Adds a relation name pattern that will be matched exclusively against heap tables, excluding indexes and other relation types.

## Definition
```c
static void append_heap_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
```

## Detailed Description
The `append_heap_pattern` function is a specialized wrapper around `append_relation_pattern_helper` that processes relation patterns with heap table restriction. By setting the `heap_only` flag to true and `btree_only` to false, this function ensures that the pattern will only match against heap tables during pg_amcheck operations. This is useful when users want to perform integrity checks specifically on table data structures while excluding indexes. The function provides a targeted approach for table-specific integrity checking scenarios.

## Parameters / Member Variables
- `pia`: PatternInfoArray pointer to the pattern information array that will be extended with the new pattern
- `pattern`: const char pointer to the relation name pattern string, potentially fully qualified
- `encoding`: int value representing the client encoding used for parsing the pattern

## Dependencies
- Functions called/Symbols referenced:
  - [append_relation_pattern_helper](append_relation_pattern_helper.md)
- Types used:
  - [PatternInfoArray](../P/PatternInfoArray.md)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:361, 365)

## Notes and Other Information
- Specialized wrapper function for heap table-specific pattern matching
- Sets heap_only flag to true and btree_only to false in the helper function call
- Used when pg_amcheck needs to focus integrity checking on heap tables only
- Excludes indexes, views, and other relation types from pattern matching
- Part of pg_amcheck's type-specific filtering system for targeted integrity checks
- Delegates actual pattern processing to append_relation_pattern_helper
- Useful for scenarios where only table data integrity needs to be verified

## Simplified Source

```c
static void append_heap_pattern(PatternInfoArray *pia, const char *pattern, int encoding) {
    // Add pattern that matches only heap tables (not indexes)
    append_relation_pattern_helper(pia, pattern, encoding, true, false);
}
```