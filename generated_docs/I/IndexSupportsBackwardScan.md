# IndexSupportsBackwardScan

## Location
src/backend/executor/execAmi.c: 602 - 634

## Overview
IndexSupportsBackwardScan is a static function that determines whether a specific index supports backward scanning by checking the index access method's capabilities.

## Definition


## Detailed Description
This function queries the system catalog to determine if an index supports backward scanning. It looks up the index relation in pg_class, retrieves the access method information, and checks the  flag in the IndexAmRoutine structure. The function is used internally by the executor to determine scan capabilities for IndexScan and IndexOnlyScan operations.

The function performs the following steps:
1. Looks up the index relation in the pg_class system catalog using the provided OID
2. Extracts the access method ID from the relation record
3. Retrieves the IndexAmRoutine structure for the access method
4. Checks the  flag to determine backward scan support
5. Cleans up allocated memory and releases system cache references

## Parameters / Member Variables
- : The OID of the index relation to check for backward scan support

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - GetIndexAmRoutineByAmId
  - pfree
  - ReleaseSysCache
- Called from (representative examples):
  - ExecSupportsBackwardScan (twice in the same file)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execAmi.c file
- The function is specifically used for IndexScan and IndexOnlyScan node types
- Error handling is included for cases where the index relation lookup fails
- Memory management is properly handled with pfree() and ReleaseSysCache() calls
- The function relies on the access method's  capability flag