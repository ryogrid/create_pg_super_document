# GetTempNamespaceState

## Location
[src/backend/catalog/namespace.c:3805-3820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3805-L3820)

## Overview
Fetches the status of the session's temporary namespace, specifically designed for conveying state to parallel workers.

## Definition

```c
void GetTempNamespaceState(Oid *tempNamespaceId, Oid *tempToastNamespaceId)
```

## Detailed Description
This function retrieves the OIDs of the current session's temporary namespace and its associated toast namespace. It's primarily intended for internal use in parallel processing scenarios where worker processes need access to the main session's temporary namespace information. The function directly accesses the global variables myTempNamespace and myTempToastNamespace to return their current values.

## Parameters / Member Variables
- `tempNamespaceId`: Output parameter that receives the OID of the session's temporary namespace (0 if no temp namespace exists)
- `tempToastNamespaceId`: Output parameter that receives the OID of the session's temporary toast namespace (0 if no temp namespace exists)

## Dependencies
- Functions called/Symbols referenced:
  - myTempNamespace (global variable)
  - myTempToastNamespace (global variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (src/backend/access/transam/parallel.c:346)
  - RangeVarGetRelid (src/include/catalog/namespace.h:162)

## Notes and Other Information
- This function is specifically designed for parallel processing and is not intended for general-purpose access
- Returns 0 for both namespace OIDs if the session has not created a temporary namespace
- Part of PostgreSQL's namespace management system for temporary objects

## Simplified Source

```c
void GetTempNamespaceState(Oid *tempNamespaceId, Oid *tempToastNamespaceId) {
    // Return current temp namespace OIDs (0 if none exist)
    *tempNamespaceId = myTempNamespace;
    *tempToastNamespaceId = myTempToastNamespace;
}
```