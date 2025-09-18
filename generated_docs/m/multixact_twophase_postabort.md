# multixact_twophase_postabort

## Location
src/backend/access/transam/multixact.c: 1927 - 1938

## Overview
This function handles multixact cleanup during the post-abort phase of two-phase commit transactions, providing identical functionality to the post-commit case.

## Definition
```c
void multixact_twophase_postabort(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is part of PostgreSQL's two-phase commit protocol handling for multixact operations during transaction abort. Interestingly, the cleanup required for an aborted prepared transaction is identical to that of a committed one, so this function simply delegates to multixact_twophase_postcommit. This design reflects the fact that once a transaction reaches the prepared state, the multixact cleanup operations are the same regardless of whether the transaction ultimately commits or aborts.

## Parameters / Member Variables
- `xid`: Transaction ID of the prepared transaction being aborted
- `info`: Additional information flags (passed through to postcommit function)
- `recdata`: Recovery data containing the MultiXactId
- `len`: Length of the recovery data, expected to be sizeof(MultiXactId)

## Dependencies
- Functions called/Symbols referenced:
  - multixact_twophase_postcommit
- Called from (representative examples):
  - Referenced in SizeOfMultiXactTruncate

## Notes and Other Information
- This function is essentially a wrapper that delegates to multixact_twophase_postcommit
- The comment explicitly notes that abort cleanup is the same as commit cleanup for multixact operations
- Part of the broader two-phase commit infrastructure in PostgreSQL
- Located in src/backend/access/transam/multixact.c:1927-1938
- Demonstrates the design principle that prepared transactions require the same cleanup regardless of final outcome