# SnapBuildAddCommittedTxn

## Location
src/backend/replication/logical/snapbuild.c: 967 - 1000

## Overview
Tracks a catalog-changing transaction that has committed by adding it to the builder's committed transaction list.

## Definition
```c
static void SnapBuildAddCommittedTxn(SnapBuild *builder, TransactionId xid)
```

## Detailed Description
This function maintains a dynamic array of committed transactions that have modified system catalogs. It is a crucial component of the snapshot building process for logical decoding, as these committed transactions determine the catalog visibility for future snapshots.

The function manages memory allocation for the committed transaction array, automatically expanding it when needed using a doubling strategy (new_size = old_size * 2 + 1). This ensures efficient memory usage while avoiding frequent reallocations.

Each transaction ID added to this list represents a transaction that has made catalog changes and successfully committed. This information is later used when building snapshots to determine which catalog modifications should be visible to other transactions during logical decoding.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the committed transaction tracking state
- `xid`: Transaction ID of the catalog-changing transaction that has committed

## Dependencies
- Functions called/Symbols referenced:
  - SnapBuild (structure type)
  - repalloc (memory reallocation function)
  - DEBUG1 (logging level constant)
  - TransactionIdIsValid (validation macro)
- Called from (representative examples):
  - SnapBuildCommitTxn (snapbuild.c:1135, 1149, 1162, 1170, 1176)

## Notes and Other Information
- Function is declared static, indicating internal use within snapbuild.c
- Uses a simple append strategy rather than maintaining sorted order (noted in TODO comment)
- Memory allocation grows exponentially to minimize reallocation overhead
- Called multiple times when processing transactions with subtransactions
- Essential for tracking catalog state changes that affect snapshot visibility
- Part of the logical decoding infrastructure that maintains consistency across concurrent transactions