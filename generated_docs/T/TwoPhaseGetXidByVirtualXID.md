# TwoPhaseGetXidByVirtualXID

## Location
src/backend/access/transam/twophase.c: 852 - 902

## Overview
TwoPhaseGetXidByVirtualXID looks up a prepared transaction's XID by searching for a matching virtual transaction ID (VXID) among transactions prepared since the last startup.

## Definition


## Detailed Description
TwoPhaseGetXidByVirtualXID searches through prepared transactions to find one that matches the given virtual transaction ID. The function only finds transactions prepared since the last database startup (not recovered transactions from previous sessions). If multiple matches are found, it returns any one of them and sets the have_more flag to indicate additional matches exist. Multiple matches would require a single process number to consume 2^32 local XIDs without an intervening database restart, which is extremely unlikely in practice.

## Parameters / Member Variables
- : The VirtualTransactionId to search for among prepared transactions
- : Output parameter set to true if multiple matching transactions are found

## Dependencies
- Functions called/Symbols referenced:
  - VirtualTransactionIdIsValid (to validate input VXID)
  - LWLockAcquire/LWLockRelease (for shared lock on TwoPhaseStateLock)
  - GetPGProcByNumber (to get process information)
  - GET_VXID_FROM_PGPROC (macro to extract VXID from process)
  - VirtualTransactionIdEquals (to compare VXIDs)
- Data structures accessed:
  - TwoPhaseState (global two-phase commit state)
  - GlobalTransaction (transaction structure)
  - [PGPROC](../P/PGPROC.md) (process information)
  - [VirtualTransactionId](../V/VirtualTransactionId.md) (virtual transaction ID structure)
- Called from:
  - XactLockForVirtualXact (in lock manager)

## Notes and Other Information
- Only finds prepared transactions created since the last startup, not recovered ones
- Returns InvalidTransactionId if no matching VXID is found
- Uses shared locking to minimize contention while searching
- Skips invalid transactions (gxact->valid check)
- Asserts that matching transactions are not from redo operations (!gxact->inredo)
- Multiple matches are theoretically possible but extremely rare in practice
- Part of PostgreSQL's transaction locking infrastructure for virtual transaction IDs