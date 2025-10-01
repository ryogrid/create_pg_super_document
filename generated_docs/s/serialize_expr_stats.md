# serialize_expr_stats

## Location
[src/backend/statistics/extended_stats.c:2275-2404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2275-L2404)

## Overview
Serializes expression statistics into an array of pg_statistic rows, converting VacAttrStats data for expressions into a format suitable for storage in PostgreSQL's statistics system.

## Definition

```c
struct a new pg_statistic tuple
		 */
		for (i = 0;
```
## Detailed Description
This function takes expression analysis data and creates an array of pg_statistic tuples that represent the statistical information gathered about expressions during ANALYZE operations. For each expression in the input array, it constructs a complete pg_statistic row with all the standard statistical fields (null fraction, width, distinct values, most common values, histograms, etc.). The function handles both valid statistics (where stats_valid is true) and invalid ones by either creating a proper tuple or adding a null entry to maintain array consistency.

The function opens the pg_statistic system catalog to get the composite type information, then iterates through each expression's statistics data. For each expression, it builds a HeapTuple containing all the pg_statistic fields, using the VacAttrStats structure to populate stakind, staop, stacoll, stanumbers, and stavalues arrays. The resulting tuples are accumulated into an array using PostgreSQL's array building infrastructure.

## Parameters / Member Variables
- : Array of AnlExprData structures containing expression analysis results and associated VacAttrStats
- : Number of expressions in the exprdata array

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), get_rel_type_id, accumArrayResult, heap_form_tuple
  - [construct_array_builtin](../c/construct_array_builtin.md), construct_array, heap_copy_tuple_as_datum
  - [makeArrayResult](../m/makeArrayResult.md), ObjectIdGetDatum, Int16GetDatum, Float4GetDatum
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
- Uses InvalidOid and InvalidAttrNumber for starelid and staattnum since these are expression statistics, not column statistics
- Handles both numeric statistics arrays (stanumbers) and value statistics arrays (stavalues) 
- Memory allocation is done in CurrentMemoryContext
- The function maintains the same structure as regular attribute statistics but marks them as expression-based
- Essential for extended statistics functionality that includes expressions beyond simple column references

## Simplified Source

```c
static Datum serialize_expr_stats(AnlExprData *exprdata, int nexprs)
{
    int exprno;
    Oid typOid;
    Relation sd;
    ArrayBuildState *astate = NULL;

    // Open pg_statistic catalog and get composite type OID
    sd = table_open(StatisticRelationId, RowExclusiveLock);
    typOid = get_rel_type_id(StatisticRelationId);
    if (!OidIsValid(typOid))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("relation \"%s\" does not have a composite type", "pg_statistic")));

    // Process each expression's statistics
    for (exprno = 0; exprno < nexprs; exprno++)
    {
        int i, k;
        VacAttrStats *stats = exprdata[exprno].vacattrstat;
        Datum values[Natts_pg_statistic];
        bool nulls[Natts_pg_statistic];
        HeapTuple stup;

        // Skip invalid statistics
        if (!stats->stats_valid)
        {
            astate = accumArrayResult(astate, (Datum) 0, true, typOid, CurrentMemoryContext);
            continue;
        }

        // Initialize all fields as non-null
        for (i = 0; i < Natts_pg_statistic; ++i)
            nulls[i] = false;

        // Set basic pg_statistic fields
        values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(InvalidOid);
        values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(InvalidAttrNumber);
        values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
        values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);

        // Fill stakind array
        i = Anum_pg_statistic_stakind1 - 1;
        for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[i++] = Int16GetDatum(stats->stakind[k]);

        // Fill staop array
        i = Anum_pg_statistic_staop1 - 1;
        for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[i++] = ObjectIdGetDatum(stats->staop[k]);

        // Fill stacoll array
        i = Anum_pg_statistic_stacoll1 - 1;
        for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[i++] = ObjectIdGetDatum(stats->stacoll[k]);

        // Fill stanumbers arrays
        i = Anum_pg_statistic_stanumbers1 - 1;
        for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
        {
            int nnum = stats->numnumbers[k];
            if (nnum > 0)
            {
                int n;
                Datum *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
                ArrayType *arry;

                for (n = 0; n < nnum; n++)
                    numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
                arry = construct_array_builtin(numdatums, nnum, FLOAT4OID);
                values[i++] = PointerGetDatum(arry);
            }
            else
            {
                nulls[i] = true;
                values[i++] = (Datum) 0;
            }
        }

        // Fill stavalues arrays
        i = Anum_pg_statistic_stavalues1 - 1;
        for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
        {
            if (stats->numvalues[k] > 0)
            {
                ArrayType *arry;
                arry = construct_array(stats->stavalues[k], stats->numvalues[k],
                                       stats->statypid[k], stats->statyplen[k],
                                       stats->statypbyval[k], stats->statypalign[k]);
                values[i++] = PointerGetDatum(arry);
            }
            else
            {
                nulls[i] = true;
                values[i++] = (Datum) 0;
            }
        }

        // Create tuple and add to result array
        stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
        astate = accumArrayResult(astate,
                                  heap_copy_tuple_as_datum(stup, RelationGetDescr(sd)),
                                  false, typOid, CurrentMemoryContext);
    }

    table_close(sd, RowExclusiveLock);
    return makeArrayResult(astate, CurrentMemoryContext);
}
```