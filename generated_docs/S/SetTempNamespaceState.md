# SetTempNamespaceState

## Location
[src/backend/catalog/namespace.c:3821-3851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3821-L3851)

## Overview
Sets the status of a session's temporary namespace, specifically designed for conveying state from a leader process to parallel workers.

## Definition


## Detailed Description
This function assigns temporary namespace OIDs from a leader process to a parallel worker process, ensuring that workers have the same notion of the search path as their leader. The function includes safety assertions to verify that the worker hasn't created its own temporary namespaces before receiving the leader's state. After setting the namespace OIDs, it invalidates the search path cache to force rebuilding with the new namespace information.

## Parameters / Member Variables
- : The OID of the temporary namespace to assign to this worker session
- : The OID of the temporary toast namespace to assign to this worker session

## Dependencies
- Functions called/Symbols referenced:
  - InvalidSubTransactionId
  - [SearchPathMatcher](SearchPathMatcher.md)
  - myTempNamespace (global variable)
  - myTempToastNamespace (global variable)
  - myTempNamespaceSubID (global variable)
  - baseSearchPathValid (global variable)
  - searchPathCacheValid (global variable)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (src/backend/access/transam/parallel.c:1503)
  - RangeVarGetRelid (src/include/catalog/namespace.h:164)

## Notes and Other Information
- This function is specifically designed for parallel processing and is not intended for general-purpose access
- Includes assertions to ensure the worker process hasn't already created temporary namespaces
- Deliberately leaves myTempNamespaceSubID as InvalidSubTransactionId to prevent workers from attempting to destroy the namespace
- Invalidates search path caches to ensure proper reconstruction with the new namespace state
- Part of PostgreSQL's parallel query execution infrastructure