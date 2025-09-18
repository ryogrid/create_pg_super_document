# heap_tuple_should_freeze

## Location
src/backend/access/heap/heapam.c: 7842 - 7949

## Overview
Determines whether a heap tuple should be frozen by checking if its transaction IDs (xmin, xmax, xvac) and MultiXact IDs are older than the freeze cutoff limits.

## Definition


## Detailed Description
This function serves as a sibling to heap_prepare_freeze_tuple and determines whether a tuple would (or should) force freezing of the heap page containing it. The function examines all transaction IDs and MultiXact IDs in the tuple header (xmin, xmax, xvac fields) against the provided freeze limits. If any XID/MXID is older than the corresponding cutoff (FreezeLimit/MultiXactCutoff), the function returns true indicating the tuple should be frozen.

The function also tracks the oldest extant XIDs/MXIDs remaining in the relation through the NoFreezePageRelfrozenXid and NoFreezePageRelminMxid parameters, which help VACUUM maintain accurate tracking of unfrozen tuples. The working assumption is that the caller won't freeze this tuple, so these trackers are only updated if the tuple contains older XIDs/MXIDs.

The function handles several special cases:
- MultiXact XIDs that may contain updater XIDs requiring individual member examination
- pg_upgrade'd MultiXacts (HEAP_LOCKED_UPGRADED) which are always frozen
- HEAP_MOVED tuples with xvac fields that are always frozen if they contain normal XIDs

## Parameters / Member Variables
- : HeapTupleHeader to examine for freeze necessity
- : VacuumCutoffs structure containing freeze limits (FreezeLimit, MultiXactCutoff, etc.)
- : Input/output parameter tracking oldest unfrozen XID in relation
- : Input/output parameter tracking oldest unfrozen MultiXact ID in relation

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetXvac
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [MultiXactIdPrecedesOrEquals](../M/MultiXactIdPrecedesOrEquals.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - HEAP_XMAX_IS_MULTI
  - HEAP_LOCKED_UPGRADED
  - HEAP_XMAX_IS_LOCKED_ONLY
  - HEAP_MOVED
- Called from (representative examples):
  - [heap_prepare_freeze_tuple](heap_prepare_freeze_tuple.md)
  - [lazy_scan_noprune](../l/lazy_scan_noprune.md)

## Notes and Other Information
- The function works in conjunction with heap_prepare_freeze_tuple, providing a way to determine freeze necessity without actually performing the freeze operation
- Used extensively by VACUUM operations to decide whether pages need freezing
- The NoFreezePageRelfrozenXid and NoFreezePageRelminMxid parameters are updated only when the assumption is that the tuple won't be frozen
- pg_upgrade'd MultiXacts are always considered for freezing regardless of their age
- xvac fields in HEAP_MOVED tuples always trigger freezing when they contain normal transaction IDs