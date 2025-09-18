# get_attnum

## Location
src/backend/utils/cache/lsyscache.c: 858 - 887

## Overview
Retrieves the attribute number (column number) from the PostgreSQL system catalog for a given relation and attribute name.

## Definition
```c
AttrNumber get_attnum(Oid relid, const char *attname)
```

## Detailed Description
This function performs a system cache lookup to retrieve the attribute number from the pg_attribute catalog table using the relation OID and attribute name as search keys. It returns the internal column number used by PostgreSQL to identify columns within a relation. If the attribute doesn't exist or has been dropped, the function returns InvalidAttrNumber instead of throwing an error.

## Parameters / Member Variables
- `relid`: Object identifier of the relation (table/view/etc.) containing the attribute
- `attname`: Name of the attribute (column name) to look up

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheAttName - performs system cache lookup using relation ID and attribute name
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - ReleaseSysCache - releases the system cache entry
  - InvalidAttrNumber - constant representing an invalid/non-existent attribute number

- Called from (representative examples):
  - expand_col_privileges (catalog/aclchk.c:1613)
  - get_object_address_attribute (catalog/objectaddress.c:1517)
  - publication_translate_columns (catalog/pg_publication.c:523)
  - ATExecDropNotNull (commands/tablecmds.c:7654)
  - transformAssignmentIndirection (parser/parse_target.c:784)

## Notes and Other Information
- Returns InvalidAttrNumber if the attribute doesn't exist or has been dropped, rather than throwing an error
- Uses the ATTNAME system cache for efficient lookups by name
- Complement to get_attname function - this converts names to numbers while get_attname converts numbers to names
- Essential for translating user-specified column names to internal attribute numbers
- Commonly used in DDL operations, column privilege checks, and query parsing
- Does not distinguish between non-existent and dropped columns - both return InvalidAttrNumber