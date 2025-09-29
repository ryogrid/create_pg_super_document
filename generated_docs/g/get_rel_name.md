# get_rel_name

## Location
[src/backend/utils/cache/lsyscache.c:1928-1951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1928-L1951)

## Overview
Returns the name of a given relation as a palloc'd string copy, or NULL if no such relation exists.

## Definition
```c
char *get_rel_name(Oid relid)
```

## Detailed Description
This function retrieves the name of a PostgreSQL relation from the system catalog given its OID. It performs a system cache lookup on the pg_class catalog to retrieve the relname field and returns a palloc'd copy of the relation name string. The function is widely used throughout PostgreSQL for converting relation OIDs back to human-readable names, particularly for error messages, logging, and user-facing output. The function includes an important warning that relation names are not unique across the entire database (they can be duplicated in different schemas), so this function should primarily be used for display purposes rather than for unique identification.

## Parameters / Member Variables
- `relid`: The OID of the relation for which to retrieve the name

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_class
- Called from (representative examples):
  - [expand_col_privileges](../e/expand_col_privileges.md)
  - [heap_truncate_check_FKs](../h/heap_truncate_check_FKs.md)
  - [reindex_index](../r/reindex_index.md)
  - [RenameConstraintById](../R/RenameConstraintById.md)
  - [ExplainTargetRel](../E/ExplainTargetRel.md)
  - [show_modifytable_info](../s/show_modifytable_info.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [do_autovacuum](../d/do_autovacuum.md)
  - [get_relation_name](get_relation_name.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)

## Notes and Other Information
- Returns a palloc'd copy of the string that must be freed by the caller
- Returns NULL if the relation OID is not found, rather than raising an error
- The function includes a critical warning that relation names are not unique database-wide
- Should primarily be used for error messages and display purposes, not for unique identification
- Part of the relation cache subsystem providing efficient access to relation metadata
- Extensively used throughout the codebase for generating user-friendly error messages and logging output

## Simplified Source

```c
// Simplified version of get_rel_name
char *
get_rel_name(Oid relid)
{
    HeapTuple tp;

    // Look up the relation in the system cache by OID
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

    if (HeapTupleIsValid(tp)) {
        // Extract relation name from the pg_class tuple
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        char *result = pstrdup(NameStr(reltup->relname));

        // Clean up and return the relation name
        ReleaseSysCache(tp);
        return result;
    } else {
        // Relation not found
        return NULL;
    }
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Maintained the exact logic flow without any changes
- Kept all essential operations (cache lookup, name extraction, cleanup)
- Preserved the important NULL return for missing relations
- No actual simplification was needed as the original function is already clean and concise