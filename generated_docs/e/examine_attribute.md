# examine_attribute

## Location
[src/backend/statistics/extended_stats.c:528-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L528-L606)

## Overview
Performs pre-analysis examination of a single column to determine if it's analyzable and creates a VacAttrStats structure containing metadata needed for statistical analysis.

## Definition

```c
struct.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
```
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
  - [VacAttrStats](../V/VacAttrStats.md) (structure allocation)
  - [SearchSysCache2](../S/SearchSysCache2.md), SearchSysCacheCopy1 (system catalog lookups)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (attribute retrieval)
  - [DatumGetInt16](../D/DatumGetInt16.md), Int16GetDatum (data conversion)
  - [exprType](exprType.md), exprTypmod, exprCollation (expression type analysis)
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

## Simplified Source

```c
static VacAttrStats *examine_attribute(Relation onerel, int attnum, Node *index_expr) {
    Form_pg_attribute attr = TupleDescAttr(onerel->rd_att, attnum - 1);
    VacAttrStats *stats;
    HeapTuple atttuple, typtuple;
    Datum dat;
    bool isnull, ok;
    int attstattarget;

    // Skip dropped columns
    if (attr->attisdropped)
        return NULL;

    // Get statistics target from pg_attribute
    atttuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(RelationGetRelid(onerel)), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(atttuple))
        elog(ERROR, "cache lookup failed for attribute %d of relation %u", attnum, RelationGetRelid(onerel));

    dat = SysCacheGetAttr(ATTNUM, atttuple, Anum_pg_attribute_attstattarget, &isnull);
    attstattarget = isnull ? -1 : DatumGetInt16(dat);
    ReleaseSysCache(atttuple);

    // Skip if user specified not to analyze (attstattarget = 0)
    if (attstattarget == 0)
        return NULL;

    // Create and initialize VacAttrStats structure
    stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
    stats->attstattarget = attstattarget;

    // Set type information based on whether this is an expression index
    if (index_expr) {
        // For expression indexes, use expression type info
        stats->attrtypid = exprType(index_expr);
        stats->attrtypmod = exprTypmod(index_expr);

        // Use explicit index collation if available, otherwise expression collation
        if (OidIsValid(onerel->rd_indcollation[attnum - 1]))
            stats->attrcollid = onerel->rd_indcollation[attnum - 1];
        else
            stats->attrcollid = exprCollation(index_expr);
    } else {
        // For regular columns, use column type info
        stats->attrtypid = attr->atttypid;
        stats->attrtypmod = attr->atttypmod;
        stats->attrcollid = attr->attcollation;
    }

    // Get type information from system catalog
    typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid));
    if (!HeapTupleIsValid(typtuple))
        elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);

    stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
    stats->anl_context = anl_context;
    stats->tupattnum = attnum;

    // Initialize statistics slots with default type information
    for (int i = 0; i < STATISTIC_NUM_SLOTS; i++) {
        stats->statypid[i] = stats->attrtypid;
        stats->statyplen[i] = stats->attrtype->typlen;
        stats->statypbyval[i] = stats->attrtype->typbyval;
        stats->statypalign[i] = stats->attrtype->typalign;
    }

    // Call type-specific analysis function
    if (OidIsValid(stats->attrtype->typanalyze))
        ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze, PointerGetDatum(stats)));
    else
        ok = std_typanalyze(stats);

    // Validate that analysis setup succeeded
    if (!ok || stats->compute_stats == NULL || stats->minrows <= 0) {
        heap_freetuple(typtuple);
        pfree(stats);
        return NULL;
    }

    return stats;
}
```