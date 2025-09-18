# heap_getnextslot

## Location
src/backend/access/heap/heapam.c: 1345 - 1374

## Overview
Retrieves the next tuple from a heap table scan and stores it in a provided TupleTableSlot, returning a boolean indicating whether a tuple was found.

## Definition


## Detailed Description
The  function is a slot-based variant of  that follows the modern PostgreSQL tuple slot interface. It performs the same core scanning logic as  but stores the result in a provided  rather than returning a  directly. The function chooses between page-mode and regular scanning based on scan flags, and handles the case where no tuple is found by clearing the slot and returning false.

This function is part of the table access method interface and provides better memory management through the slot abstraction, allowing for more efficient tuple processing in the executor.

## Parameters / Member Variables
- : The table scan descriptor (cast to HeapScanDesc internally)
- : The scan direction (ForwardScanDirection or BackwardScanDirection)  
- : The TupleTableSlot to store the retrieved tuple

## Dependencies
- Functions called/Symbols referenced:
  - heapgettup_pagemode
  - heapgettup
  - ExecClearTuple
  - pgstat_count_heap_getnext
  - ExecStoreBufferHeapTuple
- Data structures used:
  - HeapScanDesc
  - TableScanDesc
  - ScanDirection
  - TupleTableSlot
- Scan flags:
  - SO_ALLOW_PAGEMODE
- Called from (representative examples):
  - SampleHeapTupleVisible
  - HeapScanIsValid

## Notes and Other Information
- Returns  if a tuple was found and stored in the slot,  if no more tuples are available
- When no tuple is found, the function clears the slot using  to ensure clean state
- Uses  to efficiently store the tuple in the slot while maintaining buffer pin
- No locking manipulations are needed as this is handled at lower levels
- Performs the same statistics tracking as  via 
- The slot-based interface allows for better memory management and integration with the executor's tuple processing pipeline
- Unlike , this function doesn't include the heap AM validation checks, suggesting it's used in more controlled contexts