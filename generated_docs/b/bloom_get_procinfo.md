# bloom_get_procinfo

## Location
src/backend/access/brin/brin_bloom.c: 717 - 746

## Overview
Caches and returns operator class support procedure information for BRIN bloom filters, avoiding repetitive system catalog lookups.

## Definition
```c
static FmgrInfo *bloom_get_procinfo(BrinDesc *bdesc, uint16 attno, uint16 procnum)
```

## Detailed Description
This is a utility function that implements caching for operator class support procedures used by BRIN bloom filter operations. It provides an optimization layer over PostgreSQL's function manager system:

1. **Cache lookup**: Checks if the requested procedure is already cached in the BloomOpaque structure
2. **Cache miss handling**: If not cached, retrieves the procedure from the system catalogs using index_getprocid and index_getprocinfo
3. **Procedure validation**: Ensures the requested support function exists in the operator class definition
4. **Memory management**: Copies function information into the appropriate memory context for persistent caching
5. **Error handling**: Reports detailed errors when required support functions are missing

The caching mechanism significantly improves performance by avoiding repeated system catalog lookups for the same procedures.

## Parameters / Member Variables
- `bdesc`: BRIN descriptor containing index and context information
- `attno`: Attribute number (1-based) identifying which column's operator class to query
- `procnum`: Support procedure number within the operator class (e.g., PROCNUM_HASH)

## Dependencies
- Functions called/Symbols referenced:
  - index_getprocid
  - index_getprocinfo
  - RegProcedureIsValid
  - fmgr_info_copy
  - ereport
  - errcode
  - errmsg_internal
  - errdetail_internal
  - PROCNUM_BASE
- Called from (representative examples):
  - brin_bloom_add_value
  - brin_bloom_consistent

## Notes and Other Information
- This is a static utility function internal to the BRIN bloom implementation
- Uses the BloomOpaque structure attached to the BRIN descriptor for caching
- The basenum calculation (procnum - PROCNUM_BASE) normalizes procedure numbers for array indexing
- Employs PostgreSQL's function manager system (fmgr_info_copy) for proper function call setup
- Error messages use internal reporting functions, indicating this is for developer debugging rather than user-facing errors
- The caching is scoped to the lifetime of the BrinDesc structure, typically the duration of an index operation
- Memory allocation uses the BRIN descriptor's memory context (bdesc->bd_context) to ensure proper cleanup
- Essential for performance in BRIN bloom operations as hash functions are called frequently during value processing