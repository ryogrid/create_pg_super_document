# datapagemap_next

## Location
src/bin/pg_rewind/datapagemap.c: 87 - 116

## Overview
Retrieves the next set block number from a datapagemap iterator, returning false when no more blocks are found.

## Definition
bool datapagemap_next(datapagemap_iterator_t *iter, BlockNumber *blkno)

## Detailed Description
This function implements the iteration logic for traversing set bits in a datapagemap bitmap. It starts from the current iterator position (nextblkno) and searches for the next set bit in the bitmap. For each potential block number, it calculates the byte offset and bit position within that byte, checks if the bit is set, and returns the block number if found. The iterator position is advanced with each call, ensuring sequential traversal. The function returns true when a set bit is found and false when the end of the bitmap is reached.

## Parameters / Member Variables
- : Pointer to the datapagemap_iterator_t structure maintaining iteration state
- : Pointer to BlockNumber where the next found block number will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [datapagemap_iterator_t](datapagemap_iterator_t.md) (iterator structure access)
  - [datapagemap_t](datapagemap_t.md) (bitmap structure access)
- Called from (representative examples):
  - [datapagemap_print](datapagemap_print.md) (in datapagemap.c:123)
  - [calculate_totals](../c/calculate_totals.md) (in filemap.c:531)
  - [perform_rewind](../p/perform_rewind.md) (in pg_rewind.c:584)

## Notes and Other Information
- Returns true if a set block number is found, false if iteration is complete
- Updates the blkno parameter with the found block number when returning true
- Uses byte-oriented bitmap access with modulo arithmetic for bit positioning
- The iterator maintains its position across calls, enabling proper sequential traversal
- Part of the standard iterator pattern used throughout the pg_rewind utility for bitmap processing