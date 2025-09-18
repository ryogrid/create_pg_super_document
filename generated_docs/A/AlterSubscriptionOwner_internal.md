# AlterSubscriptionOwner_internal

## Location
src/backend/commands/subscriptioncmds.c: 1899 - 1958

## Overview
AlterSubscriptionOwner_internal is the internal workhorse function for changing a subscription's owner, handling all permission checks and catalog updates.

## Definition


## Detailed Description
AlterSubscriptionOwner_internal performs the core logic for changing subscription ownership. It validates that the current user has permission to perform the ownership change, ensures the new owner is valid, and updates both the catalog entry and dependency records.

The function enforces several security constraints:
1. Only the current owner can change ownership
2. Subscriptions with password_required=false can only be owned by superusers
3. The new owner must be a role that the current user can set
4. The current owner must have CREATE privileges on the database

After validation, it updates the pg_subscription catalog, adjusts the dependency records to reflect the new ownership, and wakes up related background processes to handle the ownership change promptly.

## Parameters / Member Variables
- : Open relation handle for the pg_subscription catalog table
- : HeapTuple containing the subscription record to be modified
- : OID of the new owner role for the subscription

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md): Verifies current user owns the subscription
  - check_can_set_role: Validates user can assume the new owner role
  - [object_aclcheck](../o/object_aclcheck.md): Checks CREATE permission on database
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md): Updates dependency records for new ownership
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the subscription tuple in the catalog
  - [ApplyLauncherWakeupAtCommit](ApplyLauncherWakeupAtCommit.md): Wakes up apply launcher for immediate processing
  - [LogicalRepWorkersWakeupAtCommit](../L/LogicalRepWorkersWakeupAtCommit.md): Wakes up logical replication workers
- Called from (representative examples):
  - [AlterSubscriptionOwner](AlterSubscriptionOwner.md): Public interface for subscription ownership changes in subscriptioncmds.c:1980
  - [AlterSubscriptionOwner_oid](AlterSubscriptionOwner_oid.md): OID-based ownership change interface in subscriptioncmds.c:2009

## Notes and Other Information
- Static function - internal implementation detail not exposed in headers
- Performs early return if new owner is same as current owner (no-op optimization)
- Enforces superuser restriction for password_required=false subscriptions consistently with other subscription operations
- Updates both catalog and dependency system atomically within the same transaction
- Automatically notifies background processes to handle ownership changes immediately rather than waiting for periodic checks
- Permission model aligns with database schema ownership changes (requires CREATE on database)