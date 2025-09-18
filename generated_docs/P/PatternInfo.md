# PatternInfo

## Location
[src/bin/pg_amcheck/pg_amcheck.c:32-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L32-L45)

## Overview
PatternInfo is a structure used in PostgreSQL's pg_amcheck utility to store and manage pattern matching information for database objects (databases, schemas, and relations) along with type-specific constraints.

## Definition


## Detailed Description
The PatternInfo structure is a core component of pg_amcheck's pattern matching system. It stores both the original command-line pattern and its parsed components as regular expressions for different database object levels. The structure supports hierarchical pattern matching where a single pattern can be decomposed into database, schema, and relation components. Additionally, it includes boolean flags to restrict matching to specific object types (heap tables or btree indexes) and tracks whether the pattern successfully matched any objects during processing.

## Parameters / Member Variables
- : The original, unmodified pattern string as provided on the command line
- : Regular expression for matching database names, NULL if not applicable
- : Regular expression for matching schema names, NULL if not applicable  
- : Regular expression for matching relation names, NULL if not applicable
- : Boolean flag indicating the pattern should only match heap tables
- : Boolean flag indicating the pattern should only match btree indexes
- : Boolean flag tracking whether this pattern matched any database objects

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls from struct definition)
- Called from (representative examples):
  - [PatternInfoArray](PatternInfoArray.md) (as array element type)
  - [extend_pattern_info_array](../e/extend_pattern_info_array.md)
  - [append_database_pattern](../a/append_database_pattern.md)
  - [append_schema_pattern](../a/append_schema_pattern.md)
  - [append_relation_pattern_helper](../a/append_relation_pattern_helper.md)

## Notes and Other Information
- Defined in src/bin/pg_amcheck/pg_amcheck.c:32-45
- Used exclusively within the pg_amcheck utility for pattern matching functionality
- The heap_only and btree_only flags are mutually exclusive constraints for relation matching
- Pattern parsing separates the original pattern into component regular expressions for different object hierarchy levels
- The matched flag helps track pattern utilization and can be used for reporting unmatched patterns