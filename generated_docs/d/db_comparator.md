# db_comparator

## Location
[src/backend/postmaster/autovacuum.c:1055-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1055-L1072)

## Overview
A qsort comparison function that orders autovacuum database entries by their scheduling score for proper priority-based sorting.

## Definition
static int db_comparator(const void *a, const void *b)

## Detailed Description
This function serves as a comparator for the qsort algorithm when sorting the array of autovacuum database entries. It compares two avl_dbase structures based on their adl_score field, which represents the scheduling priority assigned during database list construction. The function follows standard qsort comparator conventions, returning a negative value if the first element should precede the second, zero if they are equal, and a positive value if the second should precede the first. This ordering is crucial for maintaining proper scheduling sequence in the autovacuum launcher.

## Parameters / Member Variables
- : Pointer to the first avl_dbase structure to compare
- : Pointer to the second avl_dbase structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL 32-bit integer comparison utility)
- Data structures used:
  - [avl_dbase](../a/avl_dbase.md) (casted from void pointers)
- Called from:
  - qsort function in rebuild_database_list (line 1013 in autovacuum.c)

## Notes and Other Information
- This is a static utility function internal to the autovacuum.c module
- Follows the standard C library qsort comparator function signature and behavior
- Uses PostgreSQL's pg_cmp_s32 utility for consistent and safe 32-bit integer comparison
- Critical for ensuring databases are processed in the correct priority order
- The adl_score field represents the insertion order and priority during database list rebuilding
- Lower scores indicate higher priority (databases are processed in score order)