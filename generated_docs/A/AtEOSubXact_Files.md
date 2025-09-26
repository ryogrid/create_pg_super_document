# AtEOSubXact_Files

## Location
[src/backend/storage/file/fd.c:3129-3161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3129-L3161)

## Overview
Handles file descriptor management at the end of a subtransaction, either closing temporary files on abort or reassigning them to the parent subtransaction on commit.

## Definition
```c
void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
```

## Detailed Description
This function is called at the end of a subtransaction to properly manage file descriptors that were created during the subtransaction. The behavior depends on whether the subtransaction is being committed or aborted:

- **On commit (isCommit = true)**: Files opened by the subtransaction are reassigned to the parent subtransaction by updating their create_subid field
- **On abort (isCommit = false)**: Files opened by the subtransaction are closed and their descriptors are freed

The function iterates through all allocated file descriptors and processes those that were created in the current subtransaction (identified by matching create_subid).

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the subtransaction is being committed (true) or aborted (false)
- `mySubid`: The SubTransactionId of the subtransaction being ended
- `parentSubid`: The SubTransactionId of the parent subtransaction (used on commit to reassign files)

## Dependencies
- Functions called/Symbols referenced:
  - [FreeDesc](../F/FreeDesc.md) (to free file descriptors on abort)
  - SubTransactionId (type used for subtransaction identification)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (in src/backend/access/transam/xact.c:5133)
  - [AbortSubTransaction](AbortSubTransaction.md) (in src/backend/access/transam/xact.c:5297)

## Notes and Other Information
- The function uses a careful iteration pattern (i--) after calling FreeDesc because freeing a descriptor can shift the array contents
- File descriptors are tracked with their creating subtransaction ID to enable proper cleanup
- This is part of PostgreSQL's transactional file management system that ensures proper resource cleanup