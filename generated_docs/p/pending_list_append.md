# pending_list_append

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4461 - 4470

## Overview
Appends a TocEntry to the end of a circular doubly-linked pending list, maintaining the circular list structure.

## Definition
```c
static void pending_list_append(TocEntry *l, TocEntry *te)
```

## Detailed Description
This function adds a new TocEntry to the end of a pending list that is organized as a circular doubly-linked list. The function carefully updates the pointer linkages to maintain the circular structure while inserting the new entry just before the header node (which effectively places it at the end of the list).

The implementation follows the standard circular list insertion pattern: it updates the new entry's links to point to the appropriate neighbors, then updates the existing entries' links to include the new entry in the chain. The circular nature means that the "end" of the list is the position just before the header node.

## Parameters / Member Variables
- `l`: Pointer to the TocEntry serving as the header/sentinel node of the pending list. This should be an initialized header created by pending_list_header_init.
- `te`: Pointer to the TocEntry to be appended to the pending list. This entry will be inserted at the end of the list (just before the header node).

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry (struct type)
- Called from (representative examples):
  - restore_toc_entries_prefork (during restore preparation)
  - Functions related to TEXT_DUMPALL_HEADER processing

## Notes and Other Information
- This is a static function within pg_backup_archiver.c for internal use within the archiver module
- The function assumes the pending list has been properly initialized with pending_list_header_init
- The circular list design eliminates the need for special handling of empty lists or null pointer checks
- Entries can be in both the main TOC list and pending list simultaneously due to separate link fields
- Used during pg_dump/pg_restore operations to manage the order of operations and dependencies
- The function is located at src/bin/pg_dump/pg_backup_archiver.c:4461-4470