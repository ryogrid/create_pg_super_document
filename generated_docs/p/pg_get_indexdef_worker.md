# pg_get_indexdef_worker

## Location
[src/backend/utils/adt/ruleutils.c:1250-1567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1250-L1567)

## Overview
The internal workhorse function that decompiles PostgreSQL index definitions into readable SQL statements, supporting both regular indexes and exclusion constraints with comprehensive customization options.

## Definition
```c
static char *pg_get_indexdef_worker(Oid indexrelid, int colno,
                                   const Oid *excludeOps,
                                   bool attrsOnly, bool keysOnly,
                                   bool showTblSpc, bool inherits,
                                   int prettyFlags, bool missing_ok)
```

## Detailed Description
This comprehensive function serves as the core implementation for generating human-readable index definitions from PostgreSQL's internal catalog information. It reconstructs the complete CREATE INDEX statement or portions thereof by examining pg_index, pg_class, and pg_am system catalogs.

The function handles both regular B-tree indexes and exclusion constraints, supporting advanced features like partial indexes, expression indexes, included columns, collation specifications, operator classes, tablespace assignments, and various index options. It provides fine-grained control over output formatting and content through multiple boolean parameters.

The implementation follows a systematic approach: it first retrieves catalog information, then processes index attributes (both key and included columns), handles special cases like expressions and constraints, and finally assembles the complete definition with appropriate SQL syntax.

## Parameters / Member Variables
- `indexrelid`: OID of the index relation to decompile
- `colno`: Specific column number to focus on (0 for all columns)
- `excludeOps`: Array of exclusion operator OIDs for exclusion constraints (NULL for regular indexes)
- `attrsOnly`: If true, return only attribute definitions without CREATE INDEX wrapper
- `keysOnly`: If true, exclude non-key (INCLUDE) columns from output
- `showTblSpc`: If true, include TABLESPACE clause in output
- `inherits`: Controls whether to include ONLY keyword for partitioned indexes
- `prettyFlags`: Formatting flags controlling pretty-printing behavior
- `missing_ok`: If true, return NULL instead of error for non-existent indexes

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), SysCacheGetAttrNotNull (system catalog access)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md) (access method information)
  - [deparse_expression_pretty](../d/deparse_expression_pretty.md) (expression formatting)
  - [get_attname](../g/get_attname.md), get_atttypetypmodcoll (attribute information)
  - [generate_relation_name](../g/generate_relation_name.md), generate_qualified_relation_name (relation naming)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting)
  - [get_opclass_name](../g/get_opclass_name.md), generate_collation_name (index option formatting)
  - [flatten_reloptions](../f/flatten_reloptions.md), get_reloptions (option handling)
- Called from (representative examples):
  - [pg_get_indexdef](pg_get_indexdef.md) (public interface for complete index definitions)
  - [pg_get_indexdef_columns](pg_get_indexdef_columns.md) (key columns only)
  - [pg_get_indexdef_columns_extended](pg_get_indexdef_columns_extended.md) (configurable column definitions)
  - [pg_get_constraintdef_worker](pg_get_constraintdef_worker.md) (exclusion constraint definitions)

## Notes and Other Information
- This is a static function serving as the implementation foundation for all public index definition functions
- Supports both regular indexes and exclusion constraints through the excludeOps parameter
- Handles complex index features: expression indexes, partial indexes, included columns, custom collations, operator classes, and index options
- The function performs extensive system catalog lookups and requires appropriate locking
- Returns a palloc'd string that must be freed by the caller
- Error handling includes both immediate errors and graceful failure via missing_ok parameter
- The prettyFlags parameter controls SQL formatting for readability
- Supports both complete CREATE INDEX statements and partial definitions for specialized use cases

## Simplified Source

```c
static char *
pg_get_indexdef_worker(Oid indexrelid, int colno, const Oid *excludeOps,
                      bool attrsOnly, bool keysOnly, bool showTblSpc,
                      bool inherits, int prettyFlags, bool missing_ok)
{
    bool isConstraint = (excludeOps != NULL);
    HeapTuple ht_idx, ht_idxrel, ht_am;
    Form_pg_index idxrec;
    Form_pg_class idxrelrec;
    Form_pg_am amrec;
    List *indexprs;
    StringInfoData buf;
    char *sep;

    // Fetch pg_index tuple
    ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idx))
    {
        if (missing_ok)
            return NULL;
        elog(ERROR, "cache lookup failed for index %u", indexrelid);
    }
    idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

    // Get index metadata from catalogs
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
    ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
    idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);
    amrec = (Form_pg_am) GETSTRUCT(ht_am);

    // Get index expressions if any
    if (!heap_attisnull(ht_idx, Anum_pg_index_indexprs, NULL))
    {
        Datum exprsDatum = SysCacheGetAttrNotNull(INDEXRELID, ht_idx, Anum_pg_index_indexprs);
        char *exprsString = TextDatumGetCString(exprsDatum);
        indexprs = (List *) stringToNode(exprsString);
        pfree(exprsString);
    }
    else
        indexprs = NIL;

    initStringInfo(&buf);

    // Generate CREATE INDEX statement header
    if (!attrsOnly)
    {
        if (!isConstraint)
            appendStringInfo(&buf, "CREATE %sINDEX %s ON %s USING %s (",
                           idxrec->indisunique ? "UNIQUE " : "",
                           quote_identifier(NameStr(idxrelrec->relname)),
                           generate_relation_name(indrelid, NIL),
                           quote_identifier(NameStr(amrec->amname)));
        else
            appendStringInfo(&buf, "EXCLUDE USING %s (",
                           quote_identifier(NameStr(amrec->amname)));
    }

    // Process index attributes
    sep = "";
    for (int keyno = 0; keyno < idxrec->indnatts; keyno++)
    {
        AttrNumber attnum = idxrec->indkey.values[keyno];

        // Skip non-key attributes if keysOnly
        if (keysOnly && keyno >= idxrec->indnkeyatts)
            break;

        // Add INCLUDE clause separator for non-key columns
        if (!colno && keyno == idxrec->indnkeyatts)
        {
            appendStringInfoString(&buf, ") INCLUDE (");
            sep = "";
        }

        if (!colno)
            appendStringInfoString(&buf, sep);
        sep = ", ";

        if (attnum != 0)
        {
            // Simple column reference
            char *attname = get_attname(indrelid, attnum, false);
            if (!colno || colno == keyno + 1)
                appendStringInfoString(&buf, quote_identifier(attname));
        }
        else
        {
            // Expression index
            Node *indexkey = (Node *) lfirst(list_head(indexprs));
            indexprs = list_delete_first(indexprs);

            char *expr_str = deparse_expression_pretty(indexkey, context,
                                                     false, false, prettyFlags, 0);
            if (!colno || colno == keyno + 1)
            {
                if (looks_like_function(indexkey))
                    appendStringInfoString(&buf, expr_str);
                else
                    appendStringInfo(&buf, "(%s)", expr_str);
            }
        }

        // Add index options (collation, opclass, ordering, etc.)
        if (!attrsOnly && keyno < idxrec->indnkeyatts && (!colno || colno == keyno + 1))
        {
            // Add collation, operator class, DESC/NULLS options, exclusion operators
            // (Simplified - actual code has detailed option handling)
        }
    }

    if (!attrsOnly)
    {
        appendStringInfoChar(&buf, ')');

        // Add index options, tablespace, and WHERE clause
        if (showTblSpc)
        {
            Oid tblspc = get_rel_tablespace(indexrelid);
            if (OidIsValid(tblspc))
                appendStringInfo(&buf, " TABLESPACE %s",
                               quote_identifier(get_tablespace_name(tblspc)));
        }

        // Add partial index predicate
        if (!heap_attisnull(ht_idx, Anum_pg_index_indpred, NULL))
        {
            Datum predDatum = SysCacheGetAttrNotNull(INDEXRELID, ht_idx, Anum_pg_index_indpred);
            char *predString = TextDatumGetCString(predDatum);
            Node *node = (Node *) stringToNode(predString);

            char *pred_str = deparse_expression_pretty(node, context,
                                                     false, false, prettyFlags, 0);
            appendStringInfo(&buf, " WHERE %s", pred_str);
        }
    }

    // Cleanup
    ReleaseSysCache(ht_idx);
    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);

    return buf.data;
}
```