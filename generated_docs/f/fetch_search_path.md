# fetch_search_path

## Location
src/backend/catalog/namespace.c: 4819 - 4858

## Overview
Retrieves the active search path as a palloc'ed list of namespace OIDs, with options to include or exclude implicitly-prepended namespaces.

## Definition


## Detailed Description
This function returns the current active search path as a dynamically allocated list of namespace OIDs. It first ensures the namespace path is up-to-date by calling recomputeNamespacePath(), then handles temporary namespace creation if needed. The function provides flexibility in returning either the complete search path (including implicit namespaces like pg_catalog) or just the explicitly configured portion.

A notable side effect is that this function may trigger a CommandCounterIncrement operation if it needs to create or clean out the temporary namespace to ensure the returned path accurately reflects the actual default creation namespace.

## Parameters / Member Variables
- : Boolean flag controlling whether implicitly-prepended namespaces are included in the returned list

## Dependencies
- Functions called/Symbols referenced:
  - recomputeNamespacePath
  - AccessTempTableNamespace
  - list_copy
  - linitial_oid
  - list_delete_first
- Called from (representative examples):
  - CreateExtensionInternal
  - ObjectsInPublicationToOids
  - AfterTriggerSetState
  - current_schema
  - current_schemas
  - RangeVarGetRelid

## Notes and Other Information
- Returns a palloc'ed list that the caller must free appropriately
- May have side effects (CommandCounterIncrement) despite being used by supposedly stable functions like current_schema()
- Forces temporary namespace creation when activeTempCreationPending is true to ensure result accuracy
- When includeImplicit is false, removes leading namespaces until reaching the activeCreationNamespace
- Critical for maintaining consistent namespace resolution across different PostgreSQL subsystems
- Used extensively by schema introspection functions and object resolution routines