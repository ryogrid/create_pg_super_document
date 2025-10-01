# pg_get_partkeydef_worker

## Location
[src/backend/utils/adt/ruleutils.c:1917-2075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1917-L2075)

## Overview
Internal workhorse function that decompiles and reconstructs a partition key definition from the system catalogs, providing flexible output formatting options.

## Definition

```c
static char *
pg_get_partkeydef_worker(Oid relid, int prettyFlags,
						 bool attrsOnly, bool missing_ok)
```
## Detailed Description
This is the core implementation function for generating string representations of partition key definitions. It retrieves partition information from the  system catalog and reconstructs the partition clause syntax. The function handles different partition strategies (HASH, LIST, RANGE) and can format output in multiple modes - either as a complete "PARTITION BY" clause or just the column/expression list. It processes both simple column references and complex expressions, handling collations and operator classes appropriately.

## Parameters / Member Variables
- : Object identifier (OID) of the partitioned relation whose partition key definition should be retrieved
- : Integer flags controlling the formatting and pretty-printing of the output (derived from GET_PRETTY_FLAGS macro)
- : Boolean flag indicating whether to return only the column/expression list (true) or the full partition clause including strategy (false)
- : Boolean flag indicating whether to return NULL (true) or throw an error (false) if the relation is not found

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), SysCacheGetAttrNotNull, ReleaseSysCache (system catalog access)
  - Form_pg_partitioned_table, oidvector (data structure types)
  - [heap_attisnull](../h/heap_attisnull.md), TextDatumGetCString, stringToNode (tuple and expression processing)
  - [deparse_context_for](../d/deparse_context_for.md), get_relation_name, deparse_expression_pretty (expression deparsing)
  - [get_attname](../g/get_attname.md), quote_identifier, get_atttypetypmodcoll (attribute information)
  - [generate_collation_name](../g/generate_collation_name.md), get_opclass_name (formatting helpers)
  - [looks_like_function](../l/looks_like_function.md), exprType, exprCollation (expression analysis)
- Called from (representative examples):
  - [pg_get_partkeydef](pg_get_partkeydef.md) (public function for full partition definitions)
  - [pg_get_partkeydef_columns](pg_get_partkeydef_columns.md) (public function for column-only definitions)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Handles three partition strategies: HASH, LIST, and RANGE
- Properly formats both simple column references and complex expressions with parentheses as needed
- Manages collation specifications when they differ from the column's default collation
- Includes operator class names in the output when they are not the default for the data type
- Uses StringInfo for efficient string building
- Implements proper error handling for missing relations and malformed partition expressions
- The function reconstructs the original partition key definition from stored catalog data, not from cached parsed structures

## Simplified Source

```c
static char *
pg_get_partkeydef_worker(Oid relid, int prettyFlags, bool attrsOnly, bool missing_ok)
{
    Form_pg_partitioned_table form;
    HeapTuple tuple;
    oidvector *partclass, *partcollation;
    List *partexprs;
    StringInfoData buf;

    // Look up partition info in system catalog
    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok) return NULL;
        elog(ERROR, "cache lookup failed for partition key of %u", relid);
    }

    form = (Form_pg_partitioned_table) GETSTRUCT(tuple);

    // Get partition class and collation info
    partclass = (oidvector *) DatumGetPointer(
        SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partclass));
    partcollation = (oidvector *) DatumGetPointer(
        SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partcollation));

    // Get partition expressions if any
    if (!heap_attisnull(tuple, Anum_pg_partitioned_table_partexprs, NULL)) {
        Datum exprsDatum = SysCacheGetAttrNotNull(PARTRELID, tuple,
                                                  Anum_pg_partitioned_table_partexprs);
        char *exprsString = TextDatumGetCString(exprsDatum);
        partexprs = (List *) stringToNode(exprsString);
        pfree(exprsString);
    } else {
        partexprs = NIL;
    }

    initStringInfo(&buf);

    // Add partition strategy prefix
    if (!attrsOnly) {
        switch (form->partstrat) {
            case PARTITION_STRATEGY_HASH:
                appendStringInfoString(&buf, "HASH (");
                break;
            case PARTITION_STRATEGY_LIST:
                appendStringInfoString(&buf, "LIST (");
                break;
            case PARTITION_STRATEGY_RANGE:
                appendStringInfoString(&buf, "RANGE (");
                break;
        }
    }

    // Build column/expression list
    char *sep = "";
    ListCell *partexpr_item = list_head(partexprs);
    List *context = deparse_context_for(get_relation_name(relid), relid);

    for (int keyno = 0; keyno < form->partnatts; keyno++) {
        AttrNumber attnum = form->partattrs.values[keyno];
        appendStringInfoString(&buf, sep);
        sep = ", ";

        if (attnum != 0) {
            // Simple column reference
            char *attname = get_attname(relid, attnum, false);
            appendStringInfoString(&buf, quote_identifier(attname));
        } else {
            // Expression - deparse and add parentheses if needed
            Node *partkey = (Node *) lfirst(partexpr_item);
            partexpr_item = lnext(partexprs, partexpr_item);

            char *str = deparse_expression_pretty(partkey, context, false, false, prettyFlags, 0);
            if (looks_like_function(partkey))
                appendStringInfoString(&buf, str);
            else
                appendStringInfo(&buf, "(%s)", str);
        }

        // Add collation and operator class if not default
        if (!attrsOnly) {
            Oid partcoll = partcollation->values[keyno];
            if (OidIsValid(partcoll) && partcoll != keycolcollation)
                appendStringInfo(&buf, " COLLATE %s", generate_collation_name(partcoll));
            get_opclass_name(partclass->values[keyno], keycoltype, &buf);
        }
    }

    if (!attrsOnly)
        appendStringInfoChar(&buf, ')');

    ReleaseSysCache(tuple);
    return buf.data;
}
```