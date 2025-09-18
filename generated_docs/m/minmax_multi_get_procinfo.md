# minmax_multi_get_procinfo

## Location
src/backend/access/brin/brin_minmax_multi.c: 2863 - 2898

## Overview
Cache and return minmax-multi operator class support procedure for efficient repeated access to support functions.

## Definition


## Detailed Description
This static function provides cached access to support procedures for the minmax-multi operator class. It implements a caching mechanism to avoid repetitive system catalog lookups for support functions, storing the FmgrInfo structures in the operator class's opaque data structure.

The function checks if the requested support function is already cached, and if not, retrieves it from the system catalog using index_getprocid and index_getprocinfo. If the support function doesn't exist, it reports an error with details about the missing function.

The caching is implemented using the MinmaxMultiOpaque structure's extra_procinfos array, indexed by (procnum - PROCNUM_BASE).

## Parameters / Member Variables
- : BrinDesc pointer - BRIN index descriptor containing metadata
- : uint16 - Attribute number (1-based) for the column
- : uint16 - Support function number to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - index_getprocid
  - RegProcedureIsValid
  - fmgr_info_copy
  - index_getprocinfo
  - ereport (for error handling)
- Called from (representative examples):
  - ensure_free_space_in_buffer
  - compactify_ranges
  - brin_minmax_multi_union

## Notes and Other Information
- This is a static helper function used internally within the minmax-multi implementation
- Caches support functions to improve performance during index operations
- Throws an error if the operator class definition is missing required support functions
- Uses the PROCNUM_BASE constant to map procedure numbers to array indices
- The cached FmgrInfo structures are stored in the BRIN index's memory context