# datapagemap_iterate

## Location
src/bin/pg_rewind/datapagemap.c: 75 - 86

## Overview
Creates and initializes an iterator for traversing all set bits in a datapagemap bitmap structure.

## Definition
datapagemap_iterator_t *datapagemap_iterate(datapagemap_t *map)

## Detailed Description
This function creates a new iterator object that can be used to sequentially traverse all block numbers that have been marked in the datapagemap bitmap. The iterator maintains a reference to the original map and tracks the current position (nextblkno) for iteration. The iterator must be freed using pg_free() after use to prevent memory leaks.

## Parameters / Member Variables
- : Pointer to the datapagemap_t structure to iterate over

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (for allocating the iterator structure)
- Called from (representative examples):
  - [datapagemap_print](datapagemap_print.md) (in datapagemap.c:122)
  - [calculate_totals](../c/calculate_totals.md) (in filemap.c:530)
  - [perform_rewind](../p/perform_rewind.md) (in pg_rewind.c:583)

## Notes and Other Information
- Returns a newly allocated datapagemap_iterator_t pointer that must be freed by the caller
- The iterator starts at block number 0 and will traverse all set bits in ascending order
- Used in conjunction with datapagemap_next() to implement the iteration pattern
- Part of the pg_rewind utility's bitmap traversal system for processing modified data pages