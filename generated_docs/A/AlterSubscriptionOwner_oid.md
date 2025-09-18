# AlterSubscriptionOwner_oid

## Location
src/backend/commands/subscriptioncmds.c: 1995 - 2031

## Overview
Changes the ownership of a PostgreSQL logical replication subscription using the subscription's OID (Object Identifier) as the lookup key.

## Definition


## Detailed Description
This function serves as a public interface for changing subscription ownership when the subscription is identified by its OID rather than by name. It performs the necessary system catalog lookups to validate the subscription exists, then delegates the actual ownership change to the internal implementation function. The function handles error reporting if the specified subscription OID does not exist in the system.

## Parameters / Member Variables
- : The OID of the subscription whose ownership is to be changed
- : The OID of the new owner (user) who will own the subscription

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCacheCopy1
  - HeapTupleIsValid
  - ereport
  - [AlterSubscriptionOwner_internal](AlterSubscriptionOwner_internal.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - table_close
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md)

## Notes and Other Information
- This function is typically used during ownership reassignment operations, particularly when reassigning objects during role/user management
- It opens the subscription system catalog with RowExclusiveLock to ensure exclusive access during the ownership change
- Error handling ensures that invalid subscription OIDs are properly reported with appropriate error codes
- The function follows PostgreSQL's standard pattern of public wrapper functions that perform validation before calling internal implementation functions