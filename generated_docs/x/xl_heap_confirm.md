# xl_heap_confirm

## Location
src/include/access/heapam_xlog.h: 417 - 420

## Overview
A WAL record structure used to log the confirmation of speculative tuple insertions in PostgreSQL heap tables, marking speculative tuples as committed.

## Definition
```c
typedef struct xl_heap_confirm
{
    OffsetNumber offnum;        /* confirmed tuple's offset on page */
} xl_heap_confirm;
```

## Detailed Description
The `xl_heap_confirm` structure is a WAL record format used to log the confirmation of speculative tuple insertions in PostgreSQL's heap access method. Speculative insertions are a mechanism used to handle INSERT...ON CONFLICT operations efficiently by initially inserting tuples in a "speculative" state and then either confirming or aborting them based on conflict detection.

When a speculative insertion is confirmed (meaning no conflicts were detected), this WAL record is written to log the confirmation. The record is minimal, containing only the offset of the tuple being confirmed, as the page and other context information are provided by the WAL record infrastructure.

## Parameters / Member Variables
- `offnum`: The offset number of the tuple being confirmed within its heap page

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - heap_finish_speculative
  - heap_xlog_confirm
  - heap_desc
  - SizeOfHeapConfirm

## Notes and Other Information
- Part of PostgreSQL's speculative insertion mechanism for INSERT...ON CONFLICT
- Used to transition speculative tuples to committed state in the WAL log
- Essential for crash recovery to properly handle confirmed speculative insertions
- Minimal structure reflecting the simple nature of confirmation operations
- Works in conjunction with speculative insertion and conflict detection mechanisms
- Critical for maintaining data consistency in UPSERT-style operations