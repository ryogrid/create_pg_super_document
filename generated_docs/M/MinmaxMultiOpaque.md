# MinmaxMultiOpaque

## Location
src/backend/access/brin/brin_minmax_multi.c: 111 - 116

## Overview
MinmaxMultiOpaque is a private data structure used by the BRIN (Block Range Index) minmax-multi operator class to cache function metadata and strategy information for efficient range operations.

## Definition


## Detailed Description
MinmaxMultiOpaque serves as a cache structure for the BRIN minmax-multi operator class, which maintains minimum and maximum values for data blocks. This structure stores precomputed function manager information to avoid repeated lookups during index operations. The structure is designed to optimize performance by caching both extra support procedures and B-tree strategy procedures that are frequently used in range comparisons.

## Parameters / Member Variables
- `extra_procinfos[MINMAX_MAX_PROCNUMS]`: Array of function manager info structures for additional support procedures (currently limited to 1 procedure as defined by MINMAX_MAX_PROCNUMS)
- `cached_subtype`: Object identifier (OID) of the cached subtype, used to track the data type for which the cached function information is valid
- `strategy_procinfos[BTMaxStrategyNumber]`: Array of function manager info structures for B-tree strategy procedures (5 strategies: less than, less-equal, equal, greater-equal, greater than)

## Dependencies
- Constants referenced:
  - MINMAX_MAX_PROCNUMS (value: 1)
  - BTMaxStrategyNumber (value: 5)
- Used by functions:
  - brin_minmax_multi_opcinfo
  - minmax_multi_get_procinfo  
  - minmax_multi_get_strategy_procinfo

## Notes and Other Information
- This is an internal structure specific to the BRIN minmax-multi access method
- The structure optimizes performance by caching frequently-used function pointers to avoid repeated catalog lookups
- The cached_subtype field ensures type safety by validating that cached function info matches the current data type context
- Part of PostgreSQL's BRIN indexing infrastructure for efficient block-level range queries