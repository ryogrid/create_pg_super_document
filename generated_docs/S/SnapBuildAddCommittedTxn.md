# SnapBuildAddCommittedTxn

## Location
[src/backend/replication/logical/snapbuild.c:967-1000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L967-L1000)

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
  - [SnapBuild](SnapBuild.md) (structure type)
  - [repalloc](../r/repalloc.md) (memory reallocation function)
  - DEBUG1 (logging level constant)
  - TransactionIdIsValid (validation macro)
- Called from (representative examples):
  - [SnapBuildCommitTxn](SnapBuildCommitTxn.md) (snapbuild.c:1135, 1149, 1162, 1170, 1176)

## Notes and Other Information
- Function is declared static, indicating internal use within snapbuild.c
- Uses a simple append strategy rather than maintaining sorted order (noted in TODO comment)
- Memory allocation grows exponentially to minimize reallocation overhead
- Called multiple times when processing transactions with subtransactions
- Essential for tracking catalog state changes that affect snapshot visibility
- Part of the logical decoding infrastructure that maintains consistency across concurrent transactions

## Simplified Source

```c
static void SnapBuildAddCommittedTxn(SnapBuild *builder, TransactionId xid) {
    Assert(TransactionIdIsValid(xid));

    // Expand array if needed (double the size + 1)
    if (builder->committed.xcnt == builder->committed.xcnt_space) {
        builder->committed.xcnt_space = builder->committed.xcnt_space * 2 + 1;

        builder->committed.xip = repalloc(builder->committed.xip,
                                         builder->committed.xcnt_space * sizeof(TransactionId));
    }

    // Add committed transaction to the list
    builder->committed.xip[builder->committed.xcnt++] = xid;
}
```