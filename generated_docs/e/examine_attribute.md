# examine_attribute

## Location
src/backend/statistics/extended_stats.c: 528 - 606

## Overview
Performs pre-analysis examination of a single column to determine if it's analyzable and creates a VacAttrStats structure containing metadata needed for statistical analysis.

## Definition


## Detailed Description
The examine_attribute function is a critical component of PostgreSQL's ANALYZE command that performs initial column examination before statistical data collection. It determines whether a column should be analyzed based on various criteria and prepares the necessary data structures for the analysis process.

The function performs several key operations:
1. Validates that the column is not dropped and should be analyzed (attstattarget != 0)
2. Retrieves column metadata from the system catalogs
3. Creates and initializes a VacAttrStats structure with appropriate type information
4. Handles special cases for expression indexes where the expression type takes precedence
5. Calls the appropriate type-specific analysis function to set up analysis parameters

For expression indexes, the function uses the expression tree's type information rather than the underlying column's storage type, ensuring accurate statistical analysis of computed values.

## Parameters / Member Variables
- : The relation (table or index) being analyzed
- : The attribute number (1-based) of the column to examine
- : Optional expression tree for expression indexes; NULL for regular columns

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStats (structure allocation)
  - [SearchSysCache2](../S/SearchSysCache2.md), SearchSysCacheCopy1 (system catalog lookups)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (attribute retrieval)
  - DatumGetInt16, Int16GetDatum (data conversion)
  - exprType, exprTypmod, exprCollation (expression type analysis)
  - [std_typanalyze](../s/std_typanalyze.md) (default type analysis)
  - OidFunctionCall1 (type-specific analysis function calls)
  - [heap_freetuple](../h/heap_freetuple.md) (memory cleanup)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (main analysis driver)
  - [lookup_var_attr_stats](../l/lookup_var_attr_stats.md) (extended statistics)

## Notes and Other Information
- Returns NULL if the column should not be analyzed (dropped, attstattarget=0, or analysis setup fails)
- The function respects user-specified statistics targets via the attstattarget column attribute
- For expression indexes, collation handling prioritizes explicit index collation over expression-derived collation
- The VacAttrStats structure is initialized with default values that can be modified by type-specific analysis functions
- Memory management includes proper cleanup of system catalog tuples and allocated structures on failure paths