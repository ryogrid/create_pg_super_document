# GetForeignDataWrapper

## Location
src/backend/foreign/foreign.c: 37 - 48

## Overview
Retrieves a foreign-data wrapper object by its Object ID (OID), providing a simplified interface to access FDW information without error handling options.

## Definition


## Detailed Description
GetForeignDataWrapper is a wrapper function that provides a convenient interface to look up foreign-data wrapper objects by their OID. It internally calls GetForeignDataWrapperExtended with default flags (0), which means it will raise an error if the specified foreign-data wrapper cannot be found. This function is commonly used throughout the PostgreSQL codebase when FDW information is needed and the caller expects the wrapper to exist.

## Parameters / Member Variables
- : The Object ID (OID) of the foreign-data wrapper to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - GetForeignDataWrapperExtended
  - ForeignDataWrapper (return type)
- Called from (representative examples):
  - AlterForeignServerOwner_internal
  - AlterForeignServer
  - CreateUserMapping
  - AlterUserMapping
  - CreateForeignTable
  - ImportForeignSchema
  - ATExecAlterColumnGenericOptions
  - ATExecGenericOptions
  - GetForeignDataWrapperByName

## Notes and Other Information
- This is a convenience wrapper around GetForeignDataWrapperExtended with flags set to 0
- Will raise an ERROR if the foreign-data wrapper with the specified OID does not exist
- For cases where you need to handle missing FDWs gracefully, use GetForeignDataWrapperExtended directly with FDW_MISSING_OK flag
- Located in src/backend/foreign/foreign.c:37-48
- Returns a palloc'd ForeignDataWrapper structure that should be freed by the caller when no longer needed