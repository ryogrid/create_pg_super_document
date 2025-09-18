# _moveBefore

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1941-1965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1941-L1965)

## Overview
Moves a TOC (Table of Contents) entry to a position immediately before another specified TOC entry in the linked list.

## Definition
```c
static void _moveBefore(TocEntry *pos, TocEntry *te)
```

## Detailed Description
This is a static utility function that manipulates the doubly-linked list structure of TOC entries in the PostgreSQL dump archiver. It unlinks a given TOC entry from its current position and inserts it immediately before a specified position entry. The function performs the necessary pointer manipulations to maintain the integrity of the doubly-linked list.

The operation is performed in two phases:
1. Unlink the target entry (te) from its current position by updating the next/prev pointers of its neighbors
2. Insert the target entry before the position entry (pos) by updating all relevant pointers

This function is similar to `_moveAfter` but places the entry before the specified position instead of after it.

## Parameters / Member Variables
- `pos`: The TOC entry before which the target entry should be placed
- `te`: The TOC entry to be moved

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [SortTocFromFile](../S/SortTocFromFile.md) (used for reordering TOC entries based on file input)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_backup_archiver.c file
- The function assumes that both `pos` and `te` are valid non-NULL pointers to properly linked TOC entries
- No bounds checking or error handling is performed - the caller is responsible for ensuring valid inputs
- Unlike `_moveAfter`, this function does not take an ArchiveHandle parameter, making it slightly more streamlined
- Used primarily in TOC sorting operations where entries need to be reordered based on dependencies or user-specified ordering