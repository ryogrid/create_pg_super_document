# fetch_statentries_for_relation

## Location
[src/backend/statistics/extended_stats.c:422-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L422-L527)

## Overview
fetch_statentries_for_relation retrieves and parses all extended statistics object definitions from the pg_statistic_ext system catalog for a given relation, returning them as a list of StatExtEntry structures.

## Definition

```c
static List *
fetch_statentries_for_relation(Relation pg_statext, Oid relid)
```
## Detailed Description
This function performs a catalog scan of pg_statistic_ext to find all extended statistics objects defined on the specified relation. For each statistics object found, it extracts and parses the metadata including the object OID, schema name, object name, target columns, statistics target, enabled statistics types, and any expression definitions. The function handles the complex parsing of catalog array fields (stxkind for statistics types, stxkeys for column numbers) and deserializes expression strings back into parse trees when present. Expression parse trees are processed through eval_const_expressions and fix_opfuncids to ensure they match the planner's expected format.

## Parameters / Member Variables
- : Open relation handle for the pg_statistic_ext catalog
- : OID of the relation whose statistics objects to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [DatumGetInt16](../D/DatumGetInt16.md)
  - DatumGetArrayTypeP
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [fix_opfuncids](fix_opfuncids.md)
  - [lappend_int](../l/lappend_int.md)
  - [palloc0](../p/palloc0.md)
  - [pstrdup](../p/pstrdup.md)
- Called from:
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (in src/backend/statistics/extended_stats.c:129)
  - [ComputeExtStatisticsRows](../C/ComputeExtStatisticsRows.md) (in src/backend/statistics/extended_stats.c:285)

## Notes and Other Information
- Returns NIL if no statistics objects are defined for the relation
- Handles missing stxstattarget values by setting them to -1 (use default)
- Validates the stxkind array structure and contents with assertions
- Processes expressions through const-folding to match planner expectations
- Builds bitmapsets for column membership from the stxkeys array
- Each returned StatExtEntry contains complete metadata needed for statistics computation
- Uses system catalog indexes for efficient scanning by relation OID

## Simplified Source

```c
static List *fetch_statentries_for_relation(Relation pg_statext, Oid relid)
{
    SysScanDesc scan;
    ScanKeyData skey;
    HeapTuple htup;
    List *result = NIL;

    // Set up scan to find statistics objects for this relation
    ScanKeyInit(&skey, Anum_pg_statistic_ext_stxrelid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true, NULL, 1, &skey);

    // Process each statistics object found
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        StatExtEntry *entry;
        Datum datum;
        bool isnull;
        int i;
        ArrayType *arr;
        char *enabled;
        Form_pg_statistic_ext staForm;
        List *exprs = NIL;

        // Create entry and extract basic info
        entry = palloc0(sizeof(StatExtEntry));
        staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
        entry->statOid = staForm->oid;
        entry->schema = get_namespace_name(staForm->stxnamespace);
        entry->name = pstrdup(NameStr(staForm->stxname));

        // Extract column numbers
        for (i = 0; i < staForm->stxkeys.dim1; i++)
        {
            entry->columns = bms_add_member(entry->columns, staForm->stxkeys.values[i]);
        }

        // Get statistics target
        datum = SysCacheGetAttr(STATEXTOID, htup, Anum_pg_statistic_ext_stxstattarget, &isnull);
        entry->stattarget = isnull ? -1 : DatumGetInt16(datum);

        // Parse enabled statistics types
        datum = SysCacheGetAttrNotNull(STATEXTOID, htup, Anum_pg_statistic_ext_stxkind);
        arr = DatumGetArrayTypeP(datum);
        if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID)
            elog(ERROR, "stxkind is not a 1-D char array");
        enabled = (char *) ARR_DATA_PTR(arr);
        for (i = 0; i < ARR_DIMS(arr)[0]; i++)
        {
            entry->types = lappend_int(entry->types, (int) enabled[i]);
        }

        // Parse expressions if present
        datum = SysCacheGetAttr(STATEXTOID, htup, Anum_pg_statistic_ext_stxexprs, &isnull);
        if (!isnull)
        {
            char *exprsString = TextDatumGetCString(datum);
            exprs = (List *) stringToNode(exprsString);
            pfree(exprsString);

            // Process expressions for planner compatibility
            exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);
            fix_opfuncids((Node *) exprs);
        }

        entry->exprs = exprs;
        result = lappend(result, entry);
    }

    systable_endscan(scan);
    return result;
}
```