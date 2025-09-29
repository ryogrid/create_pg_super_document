# getRelationDescription

## Location
[src/backend/catalog/objectaddress.c:4088-4162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4088-L4162)

## Overview
A specialized subroutine that generates human-readable descriptions of PostgreSQL relations (tables, indexes, views, etc.) by examining the relation kind and formatting appropriate descriptive text.

## Definition
```c
static void getRelationDescription(StringInfo buffer, Oid relid, bool missing_ok)
```

## Detailed Description
This static helper function is called by getObjectDescription to specifically handle relation objects. It looks up the relation in pg_class, determines the appropriate descriptive prefix based on the relation kind (relkind), and appends formatted text to the provided StringInfo buffer. The function handles all types of relations including regular tables, partitioned tables, indexes, sequences, views, materialized views, composite types, foreign tables, and TOAST tables.

The function automatically qualifies relation names with schema names when the relation is not visible in the current search path. It uses PostgreSQL's visibility functions and proper identifier quoting to ensure names are displayed correctly and safely.

## Parameters / Member Variables
- `buffer`: StringInfo buffer to append the description to
- `relid`: OID of the relation to describe
- `missing_ok`: If true, return silently for missing relations instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - [RelationIsVisible](../R/RelationIsVisible.md) (visibility checking)
  - [get_namespace_name](get_namespace_name.md) (schema name retrieval)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md) (safe name quoting)
  - [appendStringInfo](../a/appendStringInfo.md) (string formatting)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class structure and relkind constants (RELKIND_RELATION, RELKIND_INDEX, etc.)

- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md) (main object description function, multiple locations)
  - object_type_map (object type mapping)

## Notes and Other Information
- Static function, only accessible within objectaddress.c
- Handles all PostgreSQL relation types through comprehensive switch statement
- Provides localized descriptions using gettext _() macro
- Automatically qualifies names when not visible in search path
- Uses proper error handling with missing_ok parameter
- Appends to existing buffer rather than returning new string
- Critical for generating user-friendly descriptions in error messages and logs
- Handles edge cases like TOAST tables and composite types
- Ensures proper memory management with system cache operations

## Simplified Source

```c
static void
getRelationDescription(StringInfo buffer, Oid relid, bool missing_ok)
{
    HeapTuple relTup;
    Form_pg_class relForm;
    char *nspname;
    char *relname;

    // Look up relation information
    relTup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(relTup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for relation %u", relid);
        return;
    }
    relForm = (Form_pg_class) GETSTRUCT(relTup);

    // Determine if schema qualification is needed
    if (RelationIsVisible(relid))
        nspname = NULL;
    else
        nspname = get_namespace_name(relForm->relnamespace);

    relname = quote_qualified_identifier(nspname, NameStr(relForm->relname));

    // Format description based on relation kind
    switch (relForm->relkind) {
        case RELKIND_RELATION:
        case RELKIND_PARTITIONED_TABLE:
            appendStringInfo(buffer, _("table %s"), relname);
            break;
        case RELKIND_INDEX:
        case RELKIND_PARTITIONED_INDEX:
            appendStringInfo(buffer, _("index %s"), relname);
            break;
        case RELKIND_SEQUENCE:
            appendStringInfo(buffer, _("sequence %s"), relname);
            break;
        case RELKIND_TOASTVALUE:
            appendStringInfo(buffer, _("toast table %s"), relname);
            break;
        case RELKIND_VIEW:
            appendStringInfo(buffer, _("view %s"), relname);
            break;
        case RELKIND_MATVIEW:
            appendStringInfo(buffer, _("materialized view %s"), relname);
            break;
        case RELKIND_COMPOSITE_TYPE:
            appendStringInfo(buffer, _("composite type %s"), relname);
            break;
        case RELKIND_FOREIGN_TABLE:
            appendStringInfo(buffer, _("foreign table %s"), relname);
            break;
        default:
            appendStringInfo(buffer, _("relation %s"), relname);
            break;
    }

    ReleaseSysCache(relTup);
}
```