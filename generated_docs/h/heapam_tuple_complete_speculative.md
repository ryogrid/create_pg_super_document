# heapam_tuple_complete_speculative

## Location
src/backend/access/heap/heapam_handler.c: 284 - 300

## Overview
Completes a speculative tuple insertion by either confirming or aborting it based on the success status, serving as a high-level wrapper for the table access method's completion mechanism.

## Definition
```c
static void heapam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken, bool succeeded)
```

## Detailed Description
This function is part of PostgreSQL's heap table access method implementation and provides the interface for completing speculative tuple insertions. Speculative insertions are used in PostgreSQL to handle unique constraint violations gracefully - a tuple is inserted "speculatively" first, then either confirmed or aborted based on whether constraint checks pass.

The function retrieves the heap tuple from the slot, then delegates to either `heap_finish_speculative` (on success) or `heap_abort_speculative` (on failure) to update the tuple's visibility status accordingly. This maintains the abstraction layer between the table access method interface and the specific heap implementation details.

## Parameters / Member Variables
- `relation`: The heap relation where the speculative tuple was inserted
- `slot`: TupleTableSlot containing the speculatively inserted tuple, with tts_tid pointing to its location
- `specToken`: Speculative insertion token (passed but not directly used in this function)
- `succeeded`: Boolean flag indicating whether the speculative insertion should be confirmed (true) or aborted (false)

## Dependencies
- Functions called/Symbols referenced:
  - ExecFetchSlotHeapTuple
  - heap_finish_speculative
  - heap_abort_speculative
- Called from (representative examples):
  - Used through table access method interface (no direct callers found in indexed code)

## Notes and Other Information
- This is a static function within heapam_handler.c, part of the heap table access method implementation
- The function handles memory management by freeing the tuple if `ExecFetchSlotHeapTuple` allocated it
- The specToken parameter is accepted for interface compatibility but not used in the heap implementation
- Part of PostgreSQL's speculative insertion mechanism used primarily for handling INSERT ... ON CONFLICT scenarios