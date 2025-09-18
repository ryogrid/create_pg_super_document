# std_typanalyze

## Location
src/backend/commands/analyze.c: 1845 - 1922

## Overview
The default type-specific analysis function that determines the appropriate statistical analysis algorithm and minimum sample size based on available operators for a given data type.

## Definition
```c
bool std_typanalyze(VacAttrStats *stats)
```

## Detailed Description
The `std_typanalyze` function serves as the standard type analysis dispatcher for PostgreSQL's ANALYZE command. It examines the data type of a column to determine which operators are available (equality and less-than), then selects the most appropriate statistical analysis algorithm based on these capabilities. The function implements a sophisticated sampling strategy based on academic research, specifically the paper "Random sampling for histogram construction: how much is enough?" by Chaudhuri, Motwani and Narasayya. It allocates and configures a StdAnalyzeData structure to store operator information that will be used by the chosen compute_stats function.

## Parameters / Member Variables
- `stats`: Pointer to VacAttrStats structure containing column information and statistics target settings

## Dependencies
- Functions called/Symbols referenced:
  - [get_sort_group_operators](../g/get_sort_group_operators.md)
  - [palloc](../p/palloc.md)
  - [get_opcode](../g/get_opcode.md)
  - [compute_scalar_stats](../c/compute_scalar_stats.md)
  - [compute_distinct_stats](../c/compute_distinct_stats.md)
  - [compute_trivial_stats](../c/compute_trivial_stats.md)
  - StdAnalyzeData (structure type)
- Called from (representative examples):
  - [examine_attribute](../e/examine_attribute.md)
  - [array_typanalyze](../a/array_typanalyze.md)

## Notes and Other Information
- Sets attstattarget to default_statistics_target if negative
- Chooses analysis algorithm based on operator availability:
  - Both equality and less-than operators available: uses compute_scalar_stats for full histogram analysis
  - Only equality operator available: uses compute_distinct_stats for distinct value analysis
  - No useful operators: uses compute_trivial_stats for basic statistics only
- Implements sampling size formula: minrows = 300 * attstattarget, based on academic research for histogram accuracy
- The sampling strategy is designed to be robust across different table sizes due to logarithmic scaling
- Allocates StdAnalyzeData structure to pass operator information to the selected compute_stats function
- Always returns true to indicate successful setup of analysis parameters