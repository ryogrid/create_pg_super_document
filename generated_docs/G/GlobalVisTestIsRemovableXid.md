# GlobalVisTestIsRemovableXid

## Location
src/backend/storage/ipc/procarray.c: 4263 - 4284

## Overview
A wrapper function that determines whether a 32-bit transaction ID (xid) can be safely removed by testing its global visibility, converting it to a full transaction ID before delegating to the full xid version.

## Definition
```c
bool GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid)
```

## Detailed Description
This function serves as a 32-bit transaction ID wrapper around `GlobalVisTestIsRemovableFullXid()`. It safely converts a 32-bit TransactionId to a FullTransactionId before performing the removability test. The function is specifically designed to work with transaction IDs from sources that are protected against XID wraparounds (such as table data protected by relfrozenxid).

The conversion from 32-bit to full transaction ID is performed safely by using the `state->definitely_needed` value as a reference point, which was established when the current snapshot was built. This allows the function to determine the correct epoch for the transaction ID without needing to acquire locks.

## Parameters / Member Variables
- `state`: Pointer to GlobalVisState containing visibility test state and reference transaction information
- `xid`: 32-bit transaction ID to test for removability (must be from a wraparound-protected source)

## Dependencies
- Functions called/Symbols referenced:
  - FullXidRelativeTo
  - GlobalVisTestIsRemovableFullXid
  - GlobalVisState (type)
  - FullTransactionId (type)
- Called from (representative examples):
  - HeapTupleSatisfiesNonVacuumable
  - HeapTupleIsSurelyDead
  - heap_page_prune_opt
  - heap_prune_satisfies_vacuum
  - vacuumRedirectAndPlaceholder
  - GlobalVisCheckRemovableXid

## Notes and Other Information
- Critical requirement: The input xid must come from a source protected against XID wraparounds
- The function avoids lock acquisition by using the pre-computed `definitely_needed` reference point
- The conversion assumes the xid is within 2 billion transactions of the reference point, which is guaranteed for wraparound-protected sources
- This is part of PostgreSQL's global visibility checking infrastructure used during vacuum and pruning operations