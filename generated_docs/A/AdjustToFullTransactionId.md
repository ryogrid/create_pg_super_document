# AdjustToFullTransactionId

## Location
src/backend/access/transam/twophase.c: 938 - 944

## Overview
Computes the FullTransactionId for a given TransactionId, safely handling epoch wraparound during two-phase commit operations.

## Definition
static inline FullTransactionId AdjustToFullTransactionId(TransactionId xid)

## Detailed Description
This static inline function converts a 32-bit TransactionId to a 64-bit FullTransactionId by combining it with the current epoch information. The function is specifically designed for use in two-phase commit scenarios where transaction IDs need to be preserved across potential epoch boundaries. It uses the current next transaction ID as a reference point to determine the correct epoch for the given XID.

The function is safe to use as long as the transaction has not yet reached COMMIT PREPARED or ROLLBACK PREPARED states, as concurrent operations like vac_truncate_clog() may invalidate the XID's allowability after those points.

## Parameters / Member Variables
- `xid`: The 32-bit TransactionId to be converted to a full transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - ReadNextFullTransactionId
  - FullTransactionIdFromAllowableAt
- Called from (representative examples):
  - [TwoPhaseFilePath](../T/TwoPhaseFilePath.md)

## Notes and Other Information
- Marked as static inline for performance optimization in two-phase commit code paths
- Contains safety assertion to ensure the input XID is valid using TransactionIdIsValid()
- The comment warns that not all callers properly limit their calls to the safe window before COMMIT/ROLLBACK PREPARED
- Critical for maintaining transaction ID integrity across epoch boundaries in prepared transactions
- Used internally within twophase.c for state file operations