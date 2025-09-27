# WalSndResourceCleanup

## Location
[src/backend/replication/walsender.c:366-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L366-L393)

## Overview
WalSndResourceCleanup properly releases and deletes a ResourceOwner that was created by WAL sender processes, ensuring systematic cleanup of all associated resources in the correct order.

## Definition
```c
void WalSndResourceCleanup(bool isCommit)
```

## Detailed Description
WalSndResourceCleanup handles the proper cleanup of ResourceOwner objects used by WAL sender processes. ResourceOwners in PostgreSQL are used to track and manage various resources (like memory, locks, files, etc.) to ensure they are properly cleaned up even in error conditions. This function follows PostgreSQL's standard resource cleanup protocol by releasing resources in three distinct phases and then deleting the ResourceOwner itself.

The function implements a careful protocol where it first saves the CurrentResourceOwner pointer locally and sets the global pointer to NULL before proceeding with cleanup. This prevents issues that could arise from attempting to delete the currently active ResourceOwner. The cleanup occurs in three phases: before locks, locks themselves, and after locks, ensuring proper ordering of resource release.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether this cleanup is part of a commit operation (true) or an abort operation (false), which may affect how certain resources are handled during cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwner](../R/ResourceOwner.md) (type)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md)
  - RESOURCE_RELEASE_BEFORE_LOCKS
  - RESOURCE_RELEASE_LOCKS
  - RESOURCE_RELEASE_AFTER_LOCKS
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md)

- Called from:
  - [perform_base_backup](../p/perform_base_backup.md) (during base backup cleanup)
  - [WalSndErrorCleanup](WalSndErrorCleanup.md) (during error recovery)
  - [UploadManifest](../U/UploadManifest.md) (during manifest upload cleanup)
  - Referenced in CRSSnapshotAction header

## Notes and Other Information
- The function safely handles the case where CurrentResourceOwner is NULL by returning early
- The three-phase resource release pattern (BEFORE_LOCKS, LOCKS, AFTER_LOCKS) is critical for proper cleanup ordering
- Setting CurrentResourceOwner to NULL before cleanup prevents potential issues with recursive cleanup attempts
- The isCommit parameter allows different cleanup behaviors for normal completion versus error conditions
- This function is specifically designed for WAL sender processes which may create ResourceOwners outside of normal transaction contexts

## Simplified Source

```c
// Simplified version of WalSndResourceCleanup
void WalSndResourceCleanup(bool isCommit) {
    // Early exit if no resource owner exists
    if (CurrentResourceOwner == NULL)
        return;

    // Save the current resource owner and clear the global pointer
    // This prevents issues with deleting the currently active resource owner
    ResourceOwner resowner = CurrentResourceOwner;
    CurrentResourceOwner = NULL;

    // Release resources in the required three-phase order
    // Phase 1: Release resources that must be freed before locks
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, isCommit, true);

    // Phase 2: Release lock resources
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_LOCKS, isCommit, true);

    // Phase 3: Release resources that must be freed after locks
    ResourceOwnerRelease(resowner, RESOURCE_RELEASE_AFTER_LOCKS, isCommit, true);

    // Finally delete the resource owner itself
    ResourceOwnerDelete(resowner);
}
```

Key simplifications made:
- Added descriptive comments explaining each phase of the cleanup process
- Combined variable declaration with assignment for clarity
- Emphasized the three-phase cleanup pattern with clear phase descriptions
- Highlighted the safety mechanism of clearing CurrentResourceOwner before cleanup
- Maintained the essential logic and error handling pattern