# GetCurrentTransactionIdIfAny

## Location
src/backend/access/transam/xact.c: 468 - 479

## Overview
Returns the transaction ID (XID) of the current sub-transaction if one is assigned, or InvalidTransactionId if not inside a transaction or the current transaction hasn't been assigned an XID yet.

## Definition


## Detailed Description
This function provides a safe way to retrieve the current transaction ID without forcing the assignment of a new XID if one doesn't exist. Unlike functions that guarantee XID assignment, this function returns InvalidTransactionId when:
- Not currently inside a transaction
- Inside a transaction that hasn't been assigned an XID yet

The function works by extracting the XID portion from the current transaction state's full transaction ID using XidFromFullTransactionId.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId
  - CurrentTransactionState->fullTransactionId
- Called from (representative examples):
  - IsSubxactTopXidLogPending (src/backend/access/transam/xact.c:575)
  - RecordTransactionAbort (src/backend/access/transam/xact.c:1725)
  - XLogRecordAssemble (src/backend/access/transam/xloginsert.c:926)
  - CHANGES_THRESHOLD (src/backend/replication/logical/reorderbuffer.c:2570)

## Notes and Other Information
- This function is safe to call when unsure if a transaction is active or has an assigned XID
- Returns InvalidTransactionId rather than forcing XID assignment, making it suitable for conditional operations
- Located in src/backend/access/transam/xact.c:468-479