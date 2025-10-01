# std_typanalyze

## Location
[src/backend/commands/analyze.c:1845-1922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1845-L1922)

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

## Simplified Source

```c
bool
std_typanalyze(VacAttrStats *stats)
{
    Oid ltopr, eqopr;
    StdAnalyzeData *mystats;

    // Use default statistics target if not specified
    if (stats->attstattarget < 0)
        stats->attstattarget = default_statistics_target;

    // Find available operators for this data type
    get_sort_group_operators(stats->attrtypid,
                           false, false, false,
                           &ltopr, &eqopr, NULL, NULL);

    // Store operator information for compute_stats functions
    mystats = palloc(sizeof(StdAnalyzeData));
    mystats->eqopr = eqopr;
    mystats->eqfunc = OidIsValid(eqopr) ? get_opcode(eqopr) : InvalidOid;
    mystats->ltopr = ltopr;
    stats->extra_data = mystats;

    // Choose analysis algorithm based on available operators
    if (OidIsValid(eqopr) && OidIsValid(ltopr))
    {
        // Full scalar analysis: both equality and ordering available
        stats->compute_stats = compute_scalar_stats;
        stats->minrows = 300 * stats->attstattarget;
    }
    else if (OidIsValid(eqopr))
    {
        // Distinct value analysis: only equality available
        stats->compute_stats = compute_distinct_stats;
        stats->minrows = 300 * stats->attstattarget;
    }
    else
    {
        // Basic analysis: no useful operators
        stats->compute_stats = compute_trivial_stats;
        stats->minrows = 300 * stats->attstattarget;
    }

    return true;
}
```