# MultiXactIdExpand

## Location
src/backend/access/transam/multixact.c: 486 - 597

## Overview
MultiXactIdExpand creates a new MultiXactId by adding a TransactionId to an existing MultiXactId, preserving only active or committed update transactions.

## Definition
MultiXactId MultiXactIdExpand(MultiXactId multi, TransactionId xid, MultiXactStatus status)

## Detailed Description
This function creates a new MultiXactId by adding a specified TransactionId with a given status to an existing MultiXactId. The function does NOT modify the existing MultiXactId; instead, it creates a completely new one to avoid race conditions with code waiting for the original MultiXactId to finish.

The function performs several key operations:
1. Retrieves all members of the input MultiXactId
2. Checks if the new transaction is already a member with the same status (returns original if so)
3. Filters existing members to keep only those that are still relevant (running transactions or committed updates)
4. Adds the new transaction to the filtered member list
5. Creates and returns a new MultiXactId from the combined member list

The filtering step is critical for freezing operations - it removes dead members that are no longer of interest, keeping only running transactions and committed update transactions.

## Parameters / Member Variables
- : The existing MultiXactId to expand
- : The TransactionId to add to the MultiXactId  
- : The MultiXactStatus to assign to the new transaction member

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - GetMultiXactIdMembers
  - TransactionIdEquals
  - TransactionIdIsInProgress
  - TransactionIdDidCommit
  - ISUPDATE_from_mxstatus
  - MultiXactIdCreateFromMembers
  - debug_elog5, debug_elog4, debug_elog3
  - mxstatus_to_string
- Called from (representative examples):
  - compute_new_xmax_infomask (src/backend/access/heap/heapam.c:5393)

## Notes and Other Information
- The function requires that MultiXactIdSetOldestMember() has been called previously
- Does not allow old/obsolete MultiXactIds as input - only currently valid ones
- Handles the edge case where the input MultiXactId becomes obsolete between caller check and function execution by creating a singleton MultiXact
- Critical for tuple freezing operations as it removes dead transaction members
- Race condition safety is maintained by creating new MultiXactIds rather than modifying existing ones
- Should not be used with MultiXactIds from clusters upgraded by pg_upgrade from older versions