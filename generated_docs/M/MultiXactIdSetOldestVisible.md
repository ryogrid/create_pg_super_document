# MultiXactIdSetOldestVisible

## Location
src/backend/access/transam/multixact.c: 729 - 769

## Overview
MultiXactIdSetOldestVisible establishes the oldest MultiXactId that the current transaction considers potentially live, protecting SLRU data from premature truncation.

## Definition
static void MultiXactIdSetOldestVisible(void)

## Detailed Description
This static function sets the OldestVisibleMXactId for the current transaction, which represents the oldest MultiXactId that this transaction might need to inspect. Once this value is set, the system guarantees that SLRU (Simple LRU) data for all MultiXactIds greater than or equal to this value will not be truncated away.

The function computes the oldest visible MultiXactId by finding the minimum value among:
1. The next MultiXactId to be assigned (MultiXactState->nextMXact)  
2. All valid OldestMemberMXactId entries across all backends

The algorithm ensures correctness through exclusive locking - by holding MultiXactGenLock exclusively, it prevents any concurrent MultiXactIdSetOldestMember calls from setting older values during the computation. This guarantees that no live transaction can be a member of any MultiXactId older than the computed OldestVisibleMXactId.

The function handles MultiXactId wraparound by ensuring the computed value is at least FirstMultiXactId.

## Parameters / Member Variables
- No parameters (operates on global state)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - LWLockAcquire (with LW_EXCLUSIVE)  
  - LWLockRelease
  - MultiXactIdPrecedes
  - debug_elog4
- Global variables accessed:
  - OldestVisibleMXactId[MyProcNumber]
  - MultiXactState->nextMXact
  - OldestMemberMXactId[]
  - FirstMultiXactId
  - MaxOldestSlot
- Called from (representative examples):
  - GetMultiXactIdMembers (src/backend/access/transam/multixact.c:1330)
  - debug_elog6 (src/backend/access/transam/multixact.c:388)

## Notes and Other Information
- Static function - only accessible within multixact.c
- Uses exclusive locking (LW_EXCLUSIVE) to ensure atomic computation across all backends
- Critical for SLRU data protection - prevents truncation of needed MultiXactId data
- Handles MultiXactId wraparound by enforcing minimum value of FirstMultiXactId
- Idempotent operation - only sets the value if not already valid
- The computed value provides a conservative estimate that ensures no required MultiXactId data is lost
- Essential for maintaining data integrity in concurrent MultiXactId operations