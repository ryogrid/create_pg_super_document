# inclusion_get_procinfo

## Location
src/backend/access/brin/brin_inclusion.c: 544 - 607

## Overview  
Static helper function that retrieves and caches BRIN inclusion operator class support procedure information with optional error handling.

## Definition
```c
static FmgrInfo *inclusion_get_procinfo(BrinDesc *bdesc, uint16 attno, uint16 procnum, bool missing_ok)
```

## Detailed Description
This function provides cached access to support procedures for BRIN inclusion operator classes. It implements a two-level caching mechanism to avoid repetitive syscache lookups: first checking if a procedure was previously searched and marked as missing, then checking if the procedure is already cached in the opaque structure. When a procedure is not cached, it attempts to look it up using the index metadata and caches the result. The function supports both strict mode (raising errors for missing procedures) and permissive mode (returning NULL for optional procedures). This design optimizes performance for frequently called support functions while providing flexible error handling.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN index descriptor containing metadata and context information
- `attno` (uint16): Attribute number (1-based) identifying the column  
- `procnum` (uint16): Support function number identifying which procedure to retrieve
- `missing_ok` (bool): If true, return NULL for missing procedures; if false, raise an error

## Dependencies
- Functions called/Symbols referenced:
  - [index_getprocid](index_getprocid.md)
  - [index_getprocinfo](index_getprocinfo.md)
  - RegProcedureIsValid
  - [fmgr_info_copy](../f/fmgr_info_copy.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg_internal](../e/errmsg_internal.md)
  - [errdetail_internal](../e/errdetail_internal.md)
- Constants:
  - PROCNUM_BASE
  - InvalidOid
  - ERRCODE_INVALID_OBJECT_DEFINITION
- Data structures:
  - [BrinDesc](../B/BrinDesc.md)
  - [InclusionOpaque](../I/InclusionOpaque.md)
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - [brin_inclusion_add_value](../b/brin_inclusion_add_value.md) (lines 182, 198, 213, 224)
  - [brin_inclusion_union](../b/brin_inclusion_union.md) (lines 508, 519)

## Notes and Other Information
- Function is declared static, limiting its visibility to the brin_inclusion.c file
- Uses basenum calculation (procnum - PROCNUM_BASE) to index into the cache arrays
- Implements negative caching by marking missing procedures in extra_proc_missing array
- Procedure information is copied to the BRIN descriptor's memory context for persistence
- Error messages include both function number and column number for debugging
- The caching mechanism prevents repeated failed lookups for non-existent optional procedures
- Support functions are essential for inclusion operations like empty testing, containment checking, mergeability testing, and union operations