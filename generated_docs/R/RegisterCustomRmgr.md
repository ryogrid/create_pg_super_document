# RegisterCustomRmgr

## Location
src/backend/access/transam/rmgr.c: 107 - 149

## Overview
Registers a new custom WAL resource manager with the PostgreSQL system, allowing extensions to handle their own WAL record types.

## Definition
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr)

## Detailed Description
RegisterCustomRmgr allows extensions to register custom WAL resource managers during PostgreSQL initialization. This function performs extensive validation to ensure the resource manager is properly configured and does not conflict with existing registrations. It must be called during shared_preload_libraries initialization.

The function validates the resource manager name, ensures the ID is in the custom range, checks for conflicts with existing resource managers, and finally registers the new resource manager in the global RmgrTable. It provides detailed error messages for various failure conditions and logs successful registrations.

## Parameters / Member Variables
- rmid: The resource manager ID to register (must be in custom range RM_MIN_CUSTOM_ID to RM_MAX_CUSTOM_ID)
- rmgr: Pointer to RmgrData structure containing the resource manager callback functions and metadata

## Dependencies
- Functions called/Symbols referenced:
  - RmgrId
  - RmgrData
  - RmgrIdIsCustom
  - RM_MIN_CUSTOM_ID
  - RM_MAX_CUSTOM_ID
  - RM_MAX_ID
  - RmgrIdExists
  - RmgrTable
  - ereport
  - process_shared_preload_libraries_in_progress
  - pg_strcasecmp
- Called from (representative examples):
  - _PG_init (in extension modules)

## Notes and Other Information
- Located in src/backend/access/transam/rmgr.c:107-149
- Must be called during shared_preload_libraries initialization, not at runtime
- Performs comprehensive validation including name uniqueness checks
- Resource manager IDs should be globally unique - developers should reserve IDs via PostgreSQL wiki
- During development, RM_EXPERIMENTAL_ID can be used to avoid reserving production IDs
- The function logs successful registrations at LOG level
- Extensions must provide a non-empty rm_name in the RmgrData structure
- Validates that the rmid is within the custom resource manager ID range