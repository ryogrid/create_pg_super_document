# get_collation_name

## Location
src/backend/utils/cache/lsyscache.c: 1035 - 1053

## Overview
Retrieves the name of a collation from the PostgreSQL system catalog given its OID, primarily used for error reporting and diagnostic purposes.

## Definition
```c
char *get_collation_name(Oid colloid)
```

## Detailed Description
The `get_collation_name` function looks up a collation by its OID in the pg_collation system catalog and returns a palloc'd copy of its name. This function is part of PostgreSQL's collation system, which handles locale-specific string comparison and sorting rules. The function is designed to be safe for use in error reporting scenarios where a human-readable collation name is needed.

The function performs a system cache lookup to efficiently retrieve the collation information. If the collation exists, it extracts the name from the catalog tuple and returns a newly allocated copy. If the collation OID is invalid or not found, it returns NULL instead of throwing an error, making it suitable for defensive programming scenarios.

## Parameters / Member Variables
- `colloid`: The OID of the collation whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (performs system cache lookup by collation OID)
  - ObjectIdGetDatum (converts OID to Datum)
  - HeapTupleIsValid (checks if cache lookup succeeded)
  - Form_pg_collation (type cast to collation catalog structure)
  - GETSTRUCT (extracts structure from heap tuple)
  - pstrdup (creates palloc'd copy of string)
  - NameStr (extracts string from Name type)
  - ReleaseSysCache (releases cache reference)
- Called from (representative examples):
  - show_sortorder_options (for EXPLAIN output)
  - MergeChildAttribute (during table inheritance)
  - MergeInheritedAttribute (during table inheritance)
  - ATExecAddColumn (during ALTER TABLE ADD COLUMN)
  - checkViewColumns (during view validation)
  - select_common_collation (during collation resolution)
  - assign_collations_walker (during query planning)

## Notes and Other Information
- Returns NULL if the collation OID is not found, rather than throwing an error
- The returned string is palloc'd and must be freed by the caller
- Collation names are not unique across different schemas, so this function should primarily be used for error messages and diagnostics
- The function explicitly warns in comments that collation names are not unique, making it unsuitable for identification purposes
- Part of PostgreSQL's internationalization infrastructure supporting locale-specific text operations