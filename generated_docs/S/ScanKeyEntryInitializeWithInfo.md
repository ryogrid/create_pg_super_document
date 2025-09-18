# ScanKeyEntryInitializeWithInfo

## Location
src/backend/access/common/scankey.c: 101 - 117

## Overview
Initializes a scan key entry using a pre-existing FmgrInfo function lookup record, providing efficiency when the function information is already available.

## Definition


## Detailed Description
ScanKeyEntryInitializeWithInfo is an optimized version of scan key initialization that accepts a pre-completed FmgrInfo structure instead of looking up the function by OID. This is particularly useful in scenarios where the same comparison function is used repeatedly, as it avoids redundant function lookups. The function copies the provided FmgrInfo using fmgr_info_copy() to ensure proper memory management and isolation. Like other scan key initialization functions, it sets all the standard scan key fields but leverages existing function manager information for improved performance.

## Parameters / Member Variables
- : Pointer to the ScanKey structure to be initialized
- : Control flags that modify scanning behavior (e.g., SK_SEARCHNULL, SK_SEARCHNOTNULL)
- : The column number (1-based) of the attribute being scanned
- : Strategy number indicating the type of comparison operation
- : OID of the subtype for polymorphic operators, or InvalidOid if not applicable
- : OID of the collation to use for string comparisons
- : Pointer to an already-completed FmgrInfo function lookup record
- : The value to compare against during scanning

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info_copy
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - _bt_first (B-tree index scanning with pre-loaded function info)
  - _bt_mkscankey (B-tree scan key construction)

## Notes and Other Information
- More efficient than ScanKeyEntryInitialize when FmgrInfo is already available
- Uses fmgr_info_copy() to properly duplicate the FmgrInfo structure in the appropriate memory context
- CurrentMemoryContext at call time should be as long-lived as the ScanKey itself
- The provided FmgrInfo must be properly initialized before calling this function
- Particularly useful in scenarios with repeated scans using the same comparison operators
- Located at src/backend/access/common/scankey.c:101-117