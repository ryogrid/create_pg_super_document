# WalSndResourceCleanup

## Location
src/backend/replication/walsender.c: 366 - 393

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
  - ResourceOwner (type)
  - ResourceOwnerRelease
  - RESOURCE_RELEASE_BEFORE_LOCKS
  - RESOURCE_RELEASE_LOCKS
  - RESOURCE_RELEASE_AFTER_LOCKS
  - ResourceOwnerDelete

- Called from:
  - perform_base_backup (during base backup cleanup)
  - WalSndErrorCleanup (during error recovery)
  - UploadManifest (during manifest upload cleanup)
  - Referenced in CRSSnapshotAction header

## Notes and Other Information
- The function safely handles the case where CurrentResourceOwner is NULL by returning early
- The three-phase resource release pattern (BEFORE_LOCKS, LOCKS, AFTER_LOCKS) is critical for proper cleanup ordering
- Setting CurrentResourceOwner to NULL before cleanup prevents potential issues with recursive cleanup attempts
- The isCommit parameter allows different cleanup behaviors for normal completion versus error conditions
- This function is specifically designed for WAL sender processes which may create ResourceOwners outside of normal transaction contexts