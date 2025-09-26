# ResourceOwnerRelease

## Location
[src/backend/utils/resowner/resowner.c:648-667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L648-L667)

## Overview
Releases all resources owned by a ResourceOwner and its descendants in a specific phase of the multi-phase cleanup process.

## Definition
```c
void ResourceOwnerRelease(ResourceOwner owner,
                          ResourceReleasePhase phase,
                          bool isCommit,
                          bool isTopLevel)
```

## Detailed Description
ResourceOwnerRelease implements PostgreSQL's multi-phase resource cleanup strategy. The function executes one specific phase of the release process, requiring multiple calls to complete full cleanup. This design preserves critical ordering requirements between different resource types and allows transaction management code to perform other operations between phases.

The function delegates the actual release work to ResourceOwnerReleaseInternal, which recursively processes the ResourceOwner and all its descendants. The multi-phase approach ensures that resources are released in the correct order: some resources must be freed before others to maintain system consistency.

The isCommit and isTopLevel parameters provide context to resource release callbacks, allowing them to behave differently during successful completion versus error recovery, and to optimize cleanup when releasing all resources at transaction end.

## Parameters / Member Variables
- `owner`: The ResourceOwner to release resources from
- `phase`: The specific ResourceReleasePhase to execute  
- `isCommit`: true for successful query/transaction completion, false for error cases
- `isTopLevel`: true when releasing TopTransactionResourceOwner at main transaction end

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerReleaseInternal](ResourceOwnerReleaseInternal.md) (performs the actual recursive release work)
  - [ResourceReleasePhase](ResourceReleasePhase.md) (enumeration of release phases)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md), AbortTransaction (transaction completion)
  - [CommitSubTransaction](../C/CommitSubTransaction.md), AbortSubTransaction (subtransaction completion)
  - [PrepareTransaction](../P/PrepareTransaction.md) (two-phase commit preparation)
  - [PortalDrop](../P/PortalDrop.md) (portal cleanup)
  - [WalSndResourceCleanup](../W/WalSndResourceCleanup.md) (WAL sender cleanup)
  - [ReleaseAuxProcessResources](ReleaseAuxProcessResources.md) (auxiliary process cleanup)

## Notes and Other Information
- Typically called three times with different phases to complete full resource cleanup
- After calling this function, no new resources can be remembered in the ResourceOwner
- [ResourceOwnerForget](ResourceOwnerForget.md) cannot be called on previously remembered resources after release starts
- The multi-phase design allows transaction code to perform operations between cleanup phases
- Includes optional statistics collection for performance monitoring when RESOWNER_STATS is enabled
- Critical for PostgreSQL's error recovery and transaction management systems
- The function marks the ResourceOwner as 'releasing' to prevent further resource registration
- Phase ordering is crucial for maintaining system consistency during cleanup