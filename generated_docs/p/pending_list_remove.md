# pending_list_remove

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4471-4481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4471-L4481)

## Overview
Removes a TocEntry from a circular doubly-linked pending list, updating the surrounding entries to maintain list integrity and clearing the removed entry's pending list pointers.

## Definition
```c
static void pending_list_remove(TocEntry *te)
```

## Detailed Description
This function removes a TocEntry from a pending list implemented as a circular doubly-linked list. The removal process involves two main steps: first, it updates the neighboring entries' pointers to bypass the entry being removed, effectively unlinking it from the list; second, it clears the removed entry's pending list pointers to NULL to indicate it's no longer part of any pending list.

The function safely handles the circular list structure by updating both the previous entry's next pointer and the next entry's previous pointer to point to each other, thus maintaining the chain integrity. After removal, the entry can potentially be added to other lists or processed independently.

## Parameters / Member Variables
- `te`: Pointer to the TocEntry to be removed from the pending list. This entry must currently be part of a pending list (i.e., its pending_prev and pending_next pointers should be valid and not NULL).

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [move_to_ready_heap](../m/move_to_ready_heap.md) (when moving entries to ready state)
  - [reduce_dependencies](../r/reduce_dependencies.md) (during dependency resolution)
  - Functions related to TEXT_DUMPALL_HEADER processing

## Notes and Other Information
- This is a static function within pg_backup_archiver.c for internal use within the archiver module
- The function assumes the entry is currently in a pending list; removing an entry not in a list could cause undefined behavior
- After removal, the entry's pending pointers are set to NULL, making it safe to check if an entry is in a pending list
- The circular list design ensures that removal operations work correctly even for single-entry lists
- Used during pg_dump/pg_restore operations when entries become ready for processing or need to be reorganized
- The function is located at src/bin/pg_dump/pg_backup_archiver.c:4471-4481