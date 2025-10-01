# pg_get_statisticsobj_worker

## Location
[src/backend/utils/adt/ruleutils.c:1634-1817](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1634-L1817)

## Overview
The internal workhorse function that decompiles an extended statistics object into its textual definition, supporting various output modes for different use cases.

## Definition
```c
static char *pg_get_statisticsobj_worker(Oid statextid, bool columns_only, bool missing_ok)
```

## Detailed Description
This is the core function responsible for reconstructing the CREATE STATISTICS command text from a statistics object stored in the system catalogs. It handles the complex process of extracting statistics object metadata, decoding column references and expressions, determining enabled statistics types, and formatting the output appropriately. The function supports multiple output modes: full CREATE STATISTICS command generation, columns-only mode for inspection purposes, and flexible error handling when objects don't exist.

The function performs several key operations:
1. Retrieves statistics object metadata from pg_statistic_ext catalog
2. Extracts and deserializes statistics expressions if present
3. Decodes enabled statistics types (ndistinct, dependencies, mcv)
4. Formats column references and expressions with proper quoting
5. Constructs the appropriate output based on the requested mode

## Parameters / Member Variables
- `statextid`: The OID of the statistics object to decompile
- `columns_only`: If true, returns only the column/expression list; if false, returns the full CREATE STATISTICS command
- `missing_ok`: If true, returns NULL when the object doesn't exist; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic_ext (system catalog structure)
  - [heap_attisnull](../h/heap_attisnull.md), SysCacheGetAttrNotNull (catalog access)
  - TextDatumGetCString, stringToNode (expression handling)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md), quote_qualified_identifier (name formatting)
  - DatumGetArrayTypeP, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE (array processing)
  - [get_attname](../g/get_attname.md), quote_identifier (column name handling)
  - [deparse_context_for](../d/deparse_context_for.md), deparse_expression_pretty (expression formatting)
  - [generate_relation_name](../g/generate_relation_name.md) (relation name formatting)
  - [looks_like_function](../l/looks_like_function.md) (expression analysis)
- Called from (representative examples):
  - [pg_get_statisticsobjdef](pg_get_statisticsobjdef.md)
  - [pg_get_statisticsobjdef_string](pg_get_statisticsobjdef_string.md)
  - [pg_get_statisticsobjdef_columns](pg_get_statisticsobjdef_columns.md)

## Notes and Other Information
- This is a static (internal) function not directly accessible from SQL
- Handles complex logic for determining when to include statistics type clauses
- Supports both simple column references and complex expressions
- Uses sophisticated expression deparsing to maintain readability
- Implements proper memory management with StringInfo buffer
- The function omits type clauses when all statistics types are enabled to ensure forward compatibility
- For single-column statistics (expression statistics), type specification is unnecessary
- Properly handles the stxkind array to determine which statistics types are enabled
- Uses deparse context to ensure proper name resolution for expressions

## Simplified Source

```c
static char *
pg_get_statisticsobj_worker(Oid statextid, bool columns_only, bool missing_ok)
{
    Form_pg_statistic_ext statextrec;
    HeapTuple        statexttup;
    StringInfoData   buf;
    List            *exprs = NIL;
    bool            has_exprs;

    // Lookup statistics object
    statexttup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statextid));
    if (!HeapTupleIsValid(statexttup)) {
        if (missing_ok)
            return NULL;
        elog(ERROR, "cache lookup failed for statistics object %u", statextid);
    }

    statextrec = (Form_pg_statistic_ext) GETSTRUCT(statexttup);
    has_exprs = !heap_attisnull(statexttup, Anum_pg_statistic_ext_stxexprs, NULL);

    // Extract expressions if present
    if (has_exprs) {
        char *exprsString = TextDatumGetCString(/* stxexprs field */);
        exprs = (List *) stringToNode(exprsString);
    }

    initStringInfo(&buf);

    // Generate CREATE STATISTICS command if not columns_only
    if (!columns_only) {
        char *nsp = get_namespace_name_or_temp(statextrec->stxnamespace);
        appendStringInfo(&buf, "CREATE STATISTICS %s",
                        quote_qualified_identifier(nsp, NameStr(statextrec->stxname)));

        // Parse enabled statistics types and add types clause if needed
        ArrayType *arr = DatumGetArrayTypeP(/* stxkind field */);
        char *enabled = (char *) ARR_DATA_PTR(arr);

        bool ndistinct_enabled = false;
        bool dependencies_enabled = false;
        bool mcv_enabled = false;

        for (int i = 0; i < ARR_DIMS(arr)[0]; i++) {
            if (enabled[i] == STATS_EXT_NDISTINCT)
                ndistinct_enabled = true;
            else if (enabled[i] == STATS_EXT_DEPENDENCIES)
                dependencies_enabled = true;
            else if (enabled[i] == STATS_EXT_MCV)
                mcv_enabled = true;
        }

        // Add types clause if not all types enabled and multi-column
        if ((!ndistinct_enabled || !dependencies_enabled || !mcv_enabled) &&
            (/* ncolumns > 1 */)) {
            appendStringInfoString(&buf, " (");
            /* append enabled type names */
            appendStringInfoChar(&buf, ')');
        }

        appendStringInfoString(&buf, " ON ");
    }

    // Output column references
    for (int colno = 0; colno < statextrec->stxkeys.dim1; colno++) {
        if (colno > 0)
            appendStringInfoString(&buf, ", ");

        AttrNumber attnum = statextrec->stxkeys.values[colno];
        char *attname = get_attname(statextrec->stxrelid, attnum, false);
        appendStringInfoString(&buf, quote_identifier(attname));
    }

    // Output expressions
    List *context = deparse_context_for(/* relation info */);
    foreach(lc, exprs) {
        Node *expr = (Node *) lfirst(lc);
        char *str = deparse_expression_pretty(expr, context, false, false,
                                            PRETTYFLAG_PAREN, 0);

        if (colno > 0)
            appendStringInfoString(&buf, ", ");

        if (looks_like_function(expr))
            appendStringInfoString(&buf, str);
        else
            appendStringInfo(&buf, "(%s)", str);
        colno++;
    }

    // Add FROM clause if full command
    if (!columns_only)
        appendStringInfo(&buf, " FROM %s",
                        generate_relation_name(statextrec->stxrelid, NIL));

    ReleaseSysCache(statexttup);
    return buf.data;
}
```