# BrinOpcInfo

## Location
src/include/access/brin_internal.h: 25 - 38

## Overview
BrinOpcInfo is a structure returned by the "OpcInfo" access method procedure that provides metadata and configuration information for a BRIN (Block Range Index) operator class.

## Definition
```c
typedef struct BrinOpcInfo
{
    /* Number of columns stored in an index column of this opclass */
    uint16      oi_nstored;

    /* Regular processing of NULLs in BrinValues? */
    bool        oi_regular_nulls;

    /* Opaque pointer for the opclass' private use */
    void       *oi_opaque;

    /* Type cache entries of the stored columns */
    TypeCacheEntry *oi_typcache[FLEXIBLE_ARRAY_MEMBER];
} BrinOpcInfo;
```

## Detailed Description
BrinOpcInfo serves as a descriptor structure that encapsulates the essential metadata needed for BRIN operator class operations. It is returned by the "OpcInfo" access method procedure and provides critical information about how data should be stored, processed, and accessed within a BRIN index for a specific operator class. The structure enables the BRIN access method to understand the storage requirements, NULL handling behavior, and type-specific operations for indexed columns.

The flexible array member design allows the structure to accommodate varying numbers of stored columns per index column, making it adaptable to different operator class implementations that may store multiple values per indexed column (such as min/max pairs, bloom filters, or inclusion sets).

## Parameters / Member Variables
- `oi_nstored`: Specifies the number of columns that will be stored in an index column for this operator class (e.g., 2 for min/max, 1 for inclusion)
- `oi_regular_nulls`: Boolean flag indicating whether this operator class follows standard NULL processing rules in BrinValues
- `oi_opaque`: Opaque pointer reserved for operator class-specific private data and configuration
- `oi_typcache`: Flexible array of TypeCacheEntry pointers providing type cache information for each stored column

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TypeCacheEntry

- Called from (representative examples):
  - brin_build_desc (src/backend/access/brin/brin.c:1574, 1593, 1601, 1608)
  - union_tuples (src/backend/access/brin/brin.c:2070, 2100)
  - brin_bloom_opcinfo (src/backend/access/brin/brin_bloom.c:451)
  - brin_inclusion_opcinfo (src/backend/access/brin/brin_inclusion.c:97)
  - brin_minmax_opcinfo (src/backend/access/brin/brin_minmax.c:37)
  - brin_minmax_multi_opcinfo (src/backend/access/brin/brin_minmax_multi.c:1861)

## Notes and Other Information
- The structure uses a flexible array member for oi_typcache, allowing dynamic sizing based on the number of stored columns
- A helper macro SizeofBrinOpcInfo(ncols) is provided to calculate the total size needed for a given number of columns
- This structure is fundamental to BRIN's extensible operator class system, enabling different indexing strategies (min/max, inclusion, bloom filters, etc.)
- The oi_opaque field allows operator classes to maintain private state and configuration data
- Memory allocation for BrinOpcInfo structures must account for the variable-length oi_typcache array