# GetCustomScanMethods

## Location
src/backend/nodes/extensible.c: 137 - 143

## Overview
Retrieves the CustomScanMethods structure for a custom scan provider by name, providing access to the callback functions needed to execute custom scans.

## Definition
```c
const CustomScanMethods *
GetCustomScanMethods(const char *CustomName, bool missing_ok)
```

## Detailed Description
GetCustomScanMethods is a lookup function that retrieves the CustomScanMethods structure associated with a named custom scan provider. This function is part of PostgreSQL's extensible node infrastructure that allows extensions to register custom scan methods. 

The function acts as a wrapper around the more general GetExtensibleNodeEntry function, specifically targeting the custom_scan_methods hash table. Custom scan providers register their methods during extension initialization, and this function allows the query planner and executor to retrieve those methods when creating and executing custom scan nodes.

The returned CustomScanMethods structure contains callback functions that define how to create execution state for the custom scan, enabling extensions to implement their own scan logic within PostgreSQL's execution framework.

## Parameters / Member Variables
- `CustomName`: A string identifier for the custom scan provider whose methods are being requested
- `missing_ok`: A boolean flag indicating whether to raise an error if the named custom scan provider is not found. If true, the function returns NULL for missing providers; if false, it raises an error.

## Dependencies
- Functions called/Symbols referenced:
  - GetExtensibleNodeEntry
  - CustomScanMethods (struct type)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- This function is located in src/backend/nodes/extensible.c at lines 137-143
- The function returns a const pointer to ensure the CustomScanMethods structure cannot be modified after retrieval
- Custom scan providers must register their methods using the extensible node registration mechanism before this function can retrieve them
- The function is part of PostgreSQL's custom scan infrastructure, which allows extensions to implement specialized scan operations beyond the built-in scan types
- The missing_ok parameter provides flexibility for callers that need to handle optional custom scan providers gracefully