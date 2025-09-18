# ts_typanalyze

## Location
src/backend/tsearch/ts_typanalyze.c: 58 - 140

## Overview
A custom typanalyze function for tsvector columns that configures statistics collection parameters for PostgreSQL's ANALYZE command.

## Definition


## Detailed Description
This function serves as a specialized analysis function for tsvector data types during PostgreSQL's ANALYZE operation. It configures the VacAttrStats structure with appropriate parameters for collecting statistics on tsvector columns, including setting the compute_stats callback to compute_tsvector_stats and determining the minimum number of rows needed for accurate statistics.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing a VacAttrStats pointer

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStats
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md)
- Called from (representative examples):
  - No direct references found (likely called via function pointer in system catalogs)

## Notes and Other Information
- Sets attstattarget to default_statistics_target if negative
- Configures minrows as 300 * attstattarget following the pattern in commands/analyze.c
- Returns true to indicate successful configuration
- Part of PostgreSQL's full-text search statistics collection system