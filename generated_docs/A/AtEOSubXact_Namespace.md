# AtEOSubXact_Namespace

## Location
src/backend/catalog/namespace.c: 4558 - 4597

## Overview
AtEOSubXact_Namespace manages temporary namespace state during subtransaction end events, either propagating namespace creation flags to parent transactions on commit or cleaning up namespace state on abort.

## Definition


## Detailed Description
This function is called at the end of a subtransaction to handle temporary namespace management. It operates based on whether the subtransaction is committing or aborting:

- **On subtransaction commit**: If the current subtransaction created a temporary namespace (myTempNamespaceSubID == mySubid), the namespace creation responsibility is transferred to the parent subtransaction by updating myTempNamespaceSubID to parentSubid.

- **On subtransaction abort**: If the current subtransaction was responsible for creating a temporary namespace, all related state is reset:
  - myTempNamespaceSubID is invalidated
  - Temporary namespace OIDs are cleared
  - Search path cache is invalidated
  - MyProc->tempNamespaceId is reset

The function ensures proper cleanup and state management for temporary namespaces across PostgreSQL's nested transaction hierarchy.

## Parameters / Member Variables
- : Boolean indicating whether the subtransaction is committing (true) or aborting (false)
- : The SubTransactionId of the current subtransaction being ended
- : The SubTransactionId of the parent subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId
  - InvalidSubTransactionId
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [AbortSubTransaction](AbortSubTransaction.md)

## Notes and Other Information
- This function is part of PostgreSQL's transaction management system that ensures temporary namespace state is properly maintained across subtransaction boundaries
- The operation of resetting MyProc->tempNamespaceId is assumed to be atomic
- Search path caches are invalidated on abort to ensure they are rebuilt with correct state
- The function handles the critical task of preventing namespace state leakage between transaction levels