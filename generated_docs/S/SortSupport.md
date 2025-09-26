# SortSupport

## Location
src/include/utils/sortsupport.h: 58 - 59

## Overview
SortSupport is a typedef pointer to the SortSupportData structure that provides a framework for accelerated sorting in PostgreSQL with reduced overhead compared to traditional comparison function calls.

## Definition
```c
typedef struct SortSupportData *SortSupport;
```

## Detailed Description
SortSupport is the primary interface for PostgreSQL's accelerated sorting framework. It serves as a pointer to a SortSupportData structure that contains all the necessary information and function pointers for optimized sorting operations. This framework allows btree opclasses to provide specialized sorting functions that can significantly improve sort performance through various optimization techniques, including abbreviated key comparisons.

The SortSupport system is designed to reduce the overhead of traditional sorting by allowing opclasses to provide custom comparison functions and optional abbreviated key generation. Instead of repeatedly invoking SQL-callable comparison functions, the framework enables direct calls to optimized C functions.

## Parameters / Member Variables
As a typedef pointer, SortSupport itself has no direct parameters, but it points to a SortSupportData structure with the following key components:
- Points to SortSupportData structure containing context, collation, sorting parameters, and function pointers
- Provides access to memory context for sort operations
- Contains collation information for locale-aware sorting
- Holds function pointers for optimized comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - SortSupportData
- Used by (representative examples):
  - ApplySortComparator
  - ApplySortAbbrevFullComparator
  - PrepareSortSupportComparisonShim
  - PrepareSortSupportFromOrderingOp
  - Various btree opclass sort support functions

## Notes and Other Information
- Defined in src/include/utils/sortsupport.h:58-59
- Part of PostgreSQL's btree sort support infrastructure
- The framework supports multiple acceleration mechanisms including abbreviated keys
- Opclasses can implement BTSORTSUPPORT_PROC to provide custom sort optimization
- The system automatically falls back to traditional comparison methods when optimizations are not available
- Critical for performance in operations involving large-scale sorting such as ORDER BY, CREATE INDEX, and merge joins