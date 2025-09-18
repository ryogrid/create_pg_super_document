# minmax_get_strategy_procinfo

## Location
src/backend/access/brin/brin_minmax.c: 261 - 314

## Overview
Caches and returns function manager information for comparison operators used in BRIN minmax operations, providing efficient access to strategy procedures.

## Definition
```c
static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype, uint16 strategynum)
```

## Detailed Description
This function serves as a caching mechanism for operator procedures used in BRIN minmax operations. It maintains a cache of FmgrInfo structures in the MinmaxOpaque structure to avoid repeated system catalog lookups for the same comparison operators. The function performs lazy initialization of operator procedures and invalidates the cache when the subtype changes.

The function looks up operators in the access method operator family (opfamily) using the AMOPSTRATEGY system catalog. It searches for operators that match the attribute type, subtype, and strategy number combination. Once found, it initializes the function manager information and caches it for subsequent use.

The caching strategy improves performance by avoiding redundant syscache lookups during index operations, which is particularly important for BRIN indexes that may perform many comparisons during summarization and consistency checking.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN descriptor containing index and operator family information
- `attno` (uint16): Attribute number (1-based) for which to get the procedure
- `subtype` (Oid): Subtype OID for polymorphic operators, or same as main type for non-polymorphic
- `strategynum` (uint16): B-tree strategy number (1-5) identifying the comparison operator

## Dependencies
- Functions called/Symbols referenced:
  - BrinDesc (structure type)
  - MinmaxOpaque (structure type for opaque data)
  - BTMaxStrategyNumber (constant defining maximum strategy numbers)
  - SearchSysCache4 (function to search system catalogs)
  - SysCacheGetAttrNotNull (function to extract catalog attributes)
  - ReleaseSysCache (function to release catalog tuple)
  - DatumGetObjectId (function to extract OID from datum)
  - get_opcode (function to get procedure OID from operator OID)
  - fmgr_info_cxt (function to initialize FmgrInfo)
  - InvalidOid (constant representing invalid OID)
  - RegProcedureIsValid (macro to validate procedure OID)

- Called from (representative examples):
  - brin_minmax_add_value:99 (for BTLessStrategyNumber comparisons)
  - brin_minmax_add_value:113 (for BTGreaterStrategyNumber comparisons)
  - brin_minmax_consistent:162, 174, 181, 188 (for various strategy comparisons)
  - brin_minmax_union:226, 239 (for min/max comparisons during union operations)

## Notes and Other Information
- The function is static and only used within the minmax operator class implementation
- Caching is invalidated whenever the subtype changes, ensuring correctness for polymorphic types
- The function mirrors the design of inclusion_get_strategy_procinfo from the inclusion operator class
- Strategy numbers must be between 1 and BTMaxStrategyNumber (typically 5 for B-tree strategies)
- Memory for FmgrInfo structures is allocated in the BRIN descriptor's memory context
- The function will error if a required operator is not found in the operator family
- Cache invalidation is necessary because different subtypes may require different operator implementations