# fetch_statentries_for_relation

## Location
src/backend/statistics/extended_stats.c: 422 - 527

## Overview
fetch_statentries_for_relation retrieves and parses all extended statistics object definitions from the pg_statistic_ext system catalog for a given relation, returning them as a list of StatExtEntry structures.

## Definition


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
  - DatumGetInt16
  - DatumGetArrayTypeP
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [fix_opfuncids](fix_opfuncids.md)
  - lappend_int
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