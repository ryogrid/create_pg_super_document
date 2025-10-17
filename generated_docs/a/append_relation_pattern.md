# append_relation_pattern

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1478-1493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1478-L1493)

## Overview
Adds a relation name pattern to be matched against both heap tables and btree indexes without type restrictions.

## Definition
```c
static void append_relation_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
```

## Detailed Description
The `append_relation_pattern` function is a simple wrapper around `append_relation_pattern_helper` that processes relation patterns for general use. It configures the pattern to match against all relation types by setting both the `heap_only` and `btree_only` flags to false. This allows the pattern to match both heap tables and btree indexes, as well as potentially other relation types during pg_amcheck operations. The function provides a convenient interface for adding patterns that should not be restricted by relation type.

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
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:344, 349)

## Notes and Other Information
- Simple wrapper function that delegates to append_relation_pattern_helper
- Designed for general relation patterns that should match all relation types
- Sets both heap_only and btree_only flags to false, allowing unrestricted matching
- Part of the pg_amcheck utility's pattern matching system
- Provides a clean interface for callers who don't need type-specific filtering
- The actual pattern processing logic is implemented in the helper function

## Simplified Source

```c
static void
append_relation_pattern(PatternInfoArray *pia, const char *pattern, int encoding)
{
    // Simple wrapper - allows matching all relation types
    append_relation_pattern_helper(pia, pattern, encoding, false, false);
}
```