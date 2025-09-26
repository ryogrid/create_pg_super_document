# VacAttrStats

## Location
[src/include/commands/vacuum.h:116-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/vacuum.h#L116-L177)

## Overview
VacAttrStats is the central data structure used during PostgreSQL's ANALYZE operation to hold statistical information about a single column or expression being analyzed.

## Definition

```c
typedef struct VacAttrStats
{
	/*
	 * These fields are set up by the main ANALYZE code before invoking the
	 * type-specific typanalyze function.  They don't necessarily match what
	 * is in pg_attribute, because some index opclasses store a different type
	 * than the underlying column/expression.  Therefore, use these fields for
	 * information about the datatype being fed to the typanalyze function.
	 */
	int			attstattarget;	/* -1 to use default */
	Oid			attrtypid;		/* type of data being analyzed */
	int32		attrtypmod;		/* typmod of data being analyzed */
	Form_pg_type attrtype;		/* copy of pg_type row for attrtypid */
	Oid			attrcollid;		/* collation of data being analyzed */
	MemoryContext anl_context;	/* where to save long-lived data */

	/*
	 * These fields must be filled in by the typanalyze routine, unless it
	 * returns false.
	 */
	AnalyzeAttrComputeStatsFunc compute_stats;	/* function pointer */
	int			minrows;		/* Minimum # of rows wanted for stats */
	void	   *extra_data;		/* for extra type-specific data */

	/*
	 * These fields are to be filled in by the compute_stats routine. (They
	 * are initialized to zero when the struct is created.)
	 */
	bool		stats_valid;
	float4		stanullfrac;	/* fraction of entries that are NULL */
	int32		stawidth;		/* average width of column values */
	float4		stadistinct;	/* # distinct values */
	int16		stakind[STATISTIC_NUM_SLOTS];
	Oid			staop[STATISTIC_NUM_SLOTS];
	Oid			stacoll[STATISTIC_NUM_SLOTS];
	int			numnumbers[STATISTIC_NUM_SLOTS];
	float4	   *stanumbers[STATISTIC_NUM_SLOTS];
	int			numvalues[STATISTIC_NUM_SLOTS];
	Datum	   *stavalues[STATISTIC_NUM_SLOTS];

	/*
	 * These fields describe the stavalues[n] element types. They will be
	 * initialized to match attrtypid, but a custom typanalyze function might
	 * want to store an array of something other than the analyzed column's
	 * elements. It should then overwrite these fields.
	 */
	Oid			statypid[STATISTIC_NUM_SLOTS];
	int16		statyplen[STATISTIC_NUM_SLOTS];
	bool		statypbyval[STATISTIC_NUM_SLOTS];
	char		statypalign[STATISTIC_NUM_SLOTS];

	/*
	 * These fields are private to the main ANALYZE code and should not be
	 * looked at by type-specific functions.
	 */
	int			tupattnum;		/* attribute number within tuples */
	HeapTuple  *rows;			/* access info for std fetch function */
	TupleDesc	tupDesc;
	Datum	   *exprvals;		/* access info for index fetch function */
	bool	   *exprnulls;
	int			rowstride;
} VacAttrStats;
```
## Detailed Description
VacAttrStats is the core data structure that orchestrates PostgreSQL's statistical analysis during ANALYZE operations. It serves multiple phases of the analysis process:

1. **Initialization Phase**: The main ANALYZE code populates type information fields
2. **Analysis Setup Phase**: Type-specific analyze functions configure computation parameters
3. **Statistical Computation Phase**: compute_stats functions populate statistical results
4. **Storage Phase**: Results are stored in pg_statistic catalog

The structure contains statistical slots (STATISTIC_NUM_SLOTS) that can hold different kinds of statistics like most common values, histograms, correlation coefficients, etc. Each slot has associated metadata describing the type of statistic and the operators used.

## Parameters / Member Variables

- `attstattarget`: Statistics target (-1 for default, 0-10000 for custom)
- `attrtypid`: OID of the data type being analyzed
- `attrtypmod`: Type modifier for the data type
- `attrtype`: Cached pg_type tuple for the data type
- `attrcollid`: Collation OID for collatable types
- `anl_context`: Memory context for long-lived statistical data
- `compute_stats`: Function pointer to compute statistical values
- `minrows`: Minimum sample rows requested for reliable statistics
- `*extra_data`: Type-specific additional data for compute_stats
- `stats_valid`: Whether any useful statistics were computed
- `stanullfrac`: Fraction of NULL values (0.0 to 1.0)
- `stawidth`: Average storage width in bytes
- `stadistinct`: Number of distinct values (negative for fraction)
- `stakind[STATISTIC_NUM_SLOTS]`: Array of statistic slot kinds (see STATISTIC_KIND_*)
- `staop[STATISTIC_NUM_SLOTS]`: Operator OIDs used for each statistic slot
- `stacoll[STATISTIC_NUM_SLOTS]`: Collation OIDs for each statistic slot
- `numnumbers[STATISTIC_NUM_SLOTS]`: Count of numeric values in each stanumbers slot
- `*stanumbers[STATISTIC_NUM_SLOTS]`: Arrays of float4 statistical values
- `numvalues[STATISTIC_NUM_SLOTS]`: Count of Datum values in each stavalues slot
- `*stavalues[STATISTIC_NUM_SLOTS]`: Arrays of Datum statistical values
- `statypid[STATISTIC_NUM_SLOTS]`: Type OID for stavalues array elements
- `statyplen[STATISTIC_NUM_SLOTS]`: Type length for stavalues elements
- `statypbyval[STATISTIC_NUM_SLOTS]`: Pass-by-value flag for stavalues elements
- `statypalign[STATISTIC_NUM_SLOTS]`: Alignment requirements for stavalues elements
- `tupattnum`: Attribute number in tuple descriptor
- `*rows`: Sample tuples for standard fetch function
- `tupDesc`: Tuple descriptor for sample data
- `*exprvals`: Computed expression values
- `*exprnulls`: NULL flags for expression values
- `rowstride`: Stride for accessing expression arrays
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (pg_type catalog form)
  - STATISTIC_NUM_SLOTS (maximum statistics slots)
  - AnalyzeAttrComputeStatsFunc (function pointer type)
  - float4 (PostgreSQL float type)
  - [MemoryContext](../M/MemoryContext.md) (memory management)

- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (src/backend/commands/analyze.c:292)
  - [compute_index_stats](../c/compute_index_stats.md) (src/backend/commands/analyze.c:928)
  - [examine_attribute](../e/examine_attribute.md) (src/backend/commands/analyze.c:1007)
  - [std_typanalyze](../s/std_typanalyze.md) (src/backend/commands/analyze.c:1845)
  - [array_typanalyze](../a/array_typanalyze.md) (src/backend/utils/adt/array_typanalyze.c:100)
  - [ts_typanalyze](../t/ts_typanalyze.md) (src/backend/tsearch/ts_typanalyze.c:60)

## Notes and Other Information
- The structure lifecycle spans the entire ANALYZE operation for a single column
- Memory allocated for statistical data should use anl_context to ensure proper cleanup
- Custom typanalyze functions can override default analysis behavior for specific types
- Statistical slots allow storing multiple types of statistics (MCVs, histograms, correlations)
- The design supports both regular table columns and index expressions
- Extended statistics (multivariate) also utilize this structure for base column statistics
- Critical for query planner's cost estimation and optimization decisions