# get_rel_name

## Location
src/backend/utils/cache/lsyscache.c: 1928 - 1951

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
  - SearchSysCache1
  - HeapTupleIsValid
  - ObjectIdGetDatum
  - GETSTRUCT
  - pstrdup
  - NameStr
  - ReleaseSysCache
  - Form_pg_class
- Called from (representative examples):
  - expand_col_privileges
  - heap_truncate_check_FKs
  - reindex_index
  - RenameConstraintById
  - ExplainTargetRel
  - show_modifytable_info
  - DefineRelation
  - ExecCheckPermissions
  - do_autovacuum
  - get_relation_name
  - SysCacheGetAttrNotNull

## Notes and Other Information
- Returns a palloc'd copy of the string that must be freed by the caller
- Returns NULL if the relation OID is not found, rather than raising an error
- The function includes a critical warning that relation names are not unique database-wide
- Should primarily be used for error messages and display purposes, not for unique identification
- Part of the relation cache subsystem providing efficient access to relation metadata
- Extensively used throughout the codebase for generating user-friendly error messages and logging output