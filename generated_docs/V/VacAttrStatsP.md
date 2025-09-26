# VacAttrStatsP

## Location
src/include/commands/vacuum.h: 106 - 115

## Overview
VacAttrStatsP is a pointer type to the VacAttrStats structure, used throughout PostgreSQL's ANALYZE operation to pass statistical analysis context between functions.

## Definition

```c
typedef struct VacAttrStats *VacAttrStatsP;
```
## Detailed Description
VacAttrStatsP serves as a convenient pointer type for the VacAttrStats structure, which is the core data structure used during PostgreSQL's ANALYZE operation. This typedef provides a cleaner interface for function signatures that need to pass or receive pointers to VacAttrStats structures.

The pointer is commonly used in:
- Type-specific analysis functions (typanalyze functions)
- Statistical computation functions (compute_stats functions) 
- Data fetching functions for accessing sample row values
- Throughout the ANALYZE infrastructure for passing statistical context

## Parameters / Member Variables
N/A - This is a simple pointer typedef to VacAttrStats struct.

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStats (the underlying struct being pointed to)
- Called from (representative examples):
  - std_fetch_func (src/backend/commands/analyze.c:1752)
  - ind_fetch_func (src/backend/commands/analyze.c:1768)
  - compute_trivial_stats (src/backend/commands/analyze.c:1923)
  - compute_distinct_stats (src/backend/commands/analyze.c:2013)
  - compute_scalar_stats (src/backend/commands/analyze.c:2356)
  - expr_fetch_func (src/backend/statistics/extended_stats.c:2234)

## Notes and Other Information
- This typedef is defined alongside function pointer types AnalyzeAttrFetchFunc and AnalyzeAttrComputeStatsFunc that also use VacAttrStatsP
- The typedef provides type safety and readability for function signatures in the ANALYZE subsystem
- Located in src/include/commands/vacuum.h:106, making it available throughout the vacuum/analyze codebase
- Part of the public API for custom type analysis functions that may be implemented by extensions