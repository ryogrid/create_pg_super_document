# VacAttrStats

## Location
src/include/commands/vacuum.h: 116 - 177

## Overview
VacAttrStats is the central data structure used during PostgreSQL's ANALYZE operation to hold statistical information about a single column or expression being analyzed.

## Definition


## Detailed Description
VacAttrStats is the core data structure that orchestrates PostgreSQL's statistical analysis during ANALYZE operations. It serves multiple phases of the analysis process:

1. **Initialization Phase**: The main ANALYZE code populates type information fields
2. **Analysis Setup Phase**: Type-specific analyze functions configure computation parameters
3. **Statistical Computation Phase**: compute_stats functions populate statistical results
4. **Storage Phase**: Results are stored in pg_statistic catalog

The structure contains statistical slots (STATISTIC_NUM_SLOTS) that can hold different kinds of statistics like most common values, histograms, correlation coefficients, etc. Each slot has associated metadata describing the type of statistic and the operators used.

## Parameters / Member Variables

### Main ANALYZE Code Fields:
- : Statistics target (-1 for default, 0-10000 for custom)
- : OID of the data type being analyzed
- : Type modifier for the data type
- : Cached pg_type tuple for the data type
- : Collation OID for collatable types
- : Memory context for long-lived statistical data

### Typanalyze Function Fields:
- : Function pointer to compute statistical values
- : Minimum sample rows requested for reliable statistics
- : Type-specific additional data for compute_stats

### Computed Statistics Fields:
- : Whether any useful statistics were computed
- : Fraction of NULL values (0.0 to 1.0)
- : Average storage width in bytes
- : Number of distinct values (negative for fraction)
- : Array of statistic slot kinds (see STATISTIC_KIND_*)
- : Operator OIDs used for each statistic slot
- : Collation OIDs for each statistic slot
- : Count of numeric values in each stanumbers slot
- : Arrays of float4 statistical values
- : Count of Datum values in each stavalues slot  
- : Arrays of Datum statistical values

### Element Type Information:
- : Type OID for stavalues array elements
- : Type length for stavalues elements
- : Pass-by-value flag for stavalues elements
- : Alignment requirements for stavalues elements

### Private Fields:
- : Attribute number in tuple descriptor
- : Sample tuples for standard fetch function
- : Tuple descriptor for sample data
- : Computed expression values
- : NULL flags for expression values
- : Stride for accessing expression arrays

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (pg_type catalog form)
  - STATISTIC_NUM_SLOTS (maximum statistics slots)
  - AnalyzeAttrComputeStatsFunc (function pointer type)
  - float4 (PostgreSQL float type)
  - MemoryContext (memory management)

- Called from (representative examples):
  - do_analyze_rel (src/backend/commands/analyze.c:292)
  - compute_index_stats (src/backend/commands/analyze.c:928)
  - examine_attribute (src/backend/commands/analyze.c:1007)
  - std_typanalyze (src/backend/commands/analyze.c:1845)
  - array_typanalyze (src/backend/utils/adt/array_typanalyze.c:100)
  - ts_typanalyze (src/backend/tsearch/ts_typanalyze.c:60)

## Notes and Other Information
- The structure lifecycle spans the entire ANALYZE operation for a single column
- Memory allocated for statistical data should use anl_context to ensure proper cleanup
- Custom typanalyze functions can override default analysis behavior for specific types
- Statistical slots allow storing multiple types of statistics (MCVs, histograms, correlations)
- The design supports both regular table columns and index expressions
- Extended statistics (multivariate) also utilize this structure for base column statistics
- Critical for query planner's cost estimation and optimization decisions