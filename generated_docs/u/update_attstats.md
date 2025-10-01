# update_attstats

## Location
[src/backend/commands/analyze.c:1609-1751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1609-L1751)

## Overview
Updates attribute statistics in the pg_statistic catalog table by inserting new or replacing existing statistical data computed during table analysis.

## Definition

```c
struct a new pg_statistic tuple
		 */
		for (i = 0;
```
## Detailed Description
The update_attstats function persists computed statistics for table columns to the pg_statistic system catalog. It processes an array of VacAttrStats structures containing statistical data collected during table analysis and either inserts new pg_statistic rows or updates existing ones.

For each valid attribute statistic, the function constructs a complete pg_statistic tuple containing the relation OID, attribute number, inheritance flag, null fraction, average width, distinct value estimate, and up to STATISTIC_NUM_SLOTS worth of detailed statistics (kinds, operators, collations, numeric arrays, and value arrays).

The function handles both regular table statistics and inheritance tree statistics (when inh=true). It uses the system cache to check for existing statistics rows and performs appropriate INSERT or UPDATE operations through the catalog tuple management functions.

## Parameters / Member Variables
- : OID of the relation whose statistics are being updated
- : Boolean indicating whether these are inheritance tree statistics
- : Number of attributes in the vacattrstats array
- : Array of pointers to VacAttrStats structures containing computed statistics

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleUpdateWithInfo](../C/CatalogTupleUpdateWithInfo.md)
  - [CatalogTupleInsertWithInfo](../C/CatalogTupleInsertWithInfo.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - [construct_array](../c/construct_array.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (called twice, once for regular stats and once for inheritance stats)

## Notes and Other Information
- Only processes attributes where stats_valid is true in the VacAttrStats structure
- Constructs complete pg_statistic tuples with all STATISTIC_NUM_SLOTS filled appropriately
- Uses catalog index state management for efficient bulk operations
- Handles both numeric arrays (stanumbers) and value arrays (stavalues) with proper type information
- Skips processing if natts <= 0 to handle empty attribute lists
- Takes RowExclusiveLock on the pg_statistic relation during updates
- Does not compute statistics for pg_statistic itself (avoided by analyze_rel logic)

## Simplified Source

```c
static void
update_attstats(Oid relid, bool inh, int natts, VacAttrStats **vacattrstats)
{
    Relation statistics_rel;
    CatalogIndexState indstate = NULL;

    if (natts <= 0)
        return;  // Nothing to do

    // Open pg_statistic catalog table for updates
    statistics_rel = table_open(StatisticRelationId, RowExclusiveLock);

    for (int attno = 0; attno < natts; attno++) {
        VacAttrStats *stats = vacattrstats[attno];

        // Skip attributes without valid statistics
        if (!stats->stats_valid)
            continue;

        // Prepare new tuple data
        Datum values[Natts_pg_statistic];
        bool nulls[Natts_pg_statistic];
        bool replaces[Natts_pg_statistic];

        // Initialize arrays
        for (int i = 0; i < Natts_pg_statistic; ++i) {
            nulls[i] = false;
            replaces[i] = true;
        }

        // Fill basic statistics fields
        values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
        values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stats->tupattnum);
        values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
        values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);

        // Fill statistic slot data (kinds, operators, collations)
        int idx = Anum_pg_statistic_stakind1 - 1;
        for (int k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[idx++] = Int16GetDatum(stats->stakind[k]);

        idx = Anum_pg_statistic_staop1 - 1;
        for (int k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[idx++] = ObjectIdGetDatum(stats->staop[k]);

        idx = Anum_pg_statistic_stacoll1 - 1;
        for (int k = 0; k < STATISTIC_NUM_SLOTS; k++)
            values[idx++] = ObjectIdGetDatum(stats->stacoll[k]);

        // Fill numeric arrays
        idx = Anum_pg_statistic_stanumbers1 - 1;
        for (int k = 0; k < STATISTIC_NUM_SLOTS; k++) {
            if (stats->numnumbers[k] > 0) {
                // Convert float array to Datum array and create array type
                Datum *numdatums = (Datum *) palloc(stats->numnumbers[k] * sizeof(Datum));
                for (int n = 0; n < stats->numnumbers[k]; n++)
                    numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
                ArrayType *array = construct_array_builtin(numdatums, stats->numnumbers[k], FLOAT4OID);
                values[idx++] = PointerGetDatum(array);
            } else {
                nulls[idx] = true;
                values[idx++] = (Datum) 0;
            }
        }

        // Fill value arrays
        idx = Anum_pg_statistic_stavalues1 - 1;
        for (int k = 0; k < STATISTIC_NUM_SLOTS; k++) {
            if (stats->numvalues[k] > 0) {
                ArrayType *array = construct_array(stats->stavalues[k],
                                                   stats->numvalues[k],
                                                   stats->statypid[k],
                                                   stats->statyplen[k],
                                                   stats->statypbyval[k],
                                                   stats->statypalign[k]);
                values[idx++] = PointerGetDatum(array);
            } else {
                nulls[idx] = true;
                values[idx++] = (Datum) 0;
            }
        }

        // Check if statistics already exist for this attribute
        HeapTuple oldtup = SearchSysCache3(STATRELATTINH,
                                           ObjectIdGetDatum(relid),
                                           Int16GetDatum(stats->tupattnum),
                                           BoolGetDatum(inh));

        // Open indexes when needed
        if (indstate == NULL)
            indstate = CatalogOpenIndexes(statistics_rel);

        HeapTuple newtup;
        if (HeapTupleIsValid(oldtup)) {
            // Update existing tuple
            newtup = heap_modify_tuple(oldtup, RelationGetDescr(statistics_rel),
                                       values, nulls, replaces);
            ReleaseSysCache(oldtup);
            CatalogTupleUpdateWithInfo(statistics_rel, &newtup->t_self, newtup, indstate);
        } else {
            // Insert new tuple
            newtup = heap_form_tuple(RelationGetDescr(statistics_rel), values, nulls);
            CatalogTupleInsertWithInfo(statistics_rel, newtup, indstate);
        }

        heap_freetuple(newtup);
    }

    // Clean up
    if (indstate != NULL)
        CatalogCloseIndexes(indstate);
    table_close(statistics_rel, RowExclusiveLock);
}
```