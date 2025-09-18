# ProcArrayClearTransaction

## Location
src/backend/storage/ipc/procarray.c: 907 - 966

## Overview
ProcArrayClearTransaction clears transaction fields from a PGPROC entry after successfully preparing a 2-phase transaction, while maintaining the transaction's visibility through the associated global transaction entry.

## Definition


## Detailed Description
This function is specifically designed for 2-phase commit (2PC) transactions. After a transaction is successfully prepared, this function clears the transaction fields from the process's PGPROC entry without actually removing the transaction from the running transaction list. The transaction remains visible as running through its associated global transaction (gxact) entry in the ProcArray.

The function clears the XID, virtual XID, xmin, and subtransaction information from the PGPROC, and increments the transaction completion count to ensure proper snapshot behavior. This prevents snapshot reuse issues that could occur if the prepared transaction wasn't properly accounted for.

## Parameters / Member Variables
- : Pointer to the PGPROC structure whose transaction fields need to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - InvalidTransactionId
  - InvalidLocalTransactionId
  - PROC_VACUUM_STATE_MASK
- Called from (representative examples):
  - PrepareTransaction

## Notes and Other Information
- Exclusively used in 2-phase commit scenarios after successful transaction preparation
- Requires exclusive ProcArrayLock to maintain consistency of xactCompletionCount
- Does not actually remove the transaction from the running set - the gxact entry keeps it visible
- Clears both main transaction and subtransaction information
- Increments xactCompletionCount to prevent problematic snapshot reuse
- Could potentially be optimized with atomic variables, but 2PC overhead makes this unnecessary
- May be combined with subsequent ProcArrayRemove() in future optimizations