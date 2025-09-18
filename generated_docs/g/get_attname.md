# get_attname

## Location
src/backend/utils/cache/lsyscache.c: 827 - 857

## Overview
Retrieves the attribute name from the PostgreSQL system catalog for a given relation and attribute number, returning it as a palloc'ed string.

## Definition


## Detailed Description
This function performs a system cache lookup to retrieve the attribute name (column name) from the pg_attribute catalog table. It searches using the relation OID and attribute number as keys. The function returns a newly allocated string containing the attribute name, which must be freed by the caller. If the attribute is not found, the behavior depends on the missing_ok parameter - it either returns NULL or throws an error.

## Parameters / Member Variables
- : Object identifier of the relation (table/view/etc.) containing the attribute
- : Attribute number (column number) within the relation, typically starting from 1
- : If true, returns NULL when attribute is not found; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2 - performs the system cache lookup using ATTNUM cache
  - Int16GetDatum - converts attribute number to Datum format
  - ObjectIdGetDatum - converts relation OID to Datum format
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - NameStr - extracts string from Name type
  - pstrdup - creates a palloc'ed copy of the string
  - ReleaseSysCache - releases the system cache entry
  - elog - logs error messages

- Called from (representative examples):
  - AddRelationNewConstraints (catalog/heap.c:2495)
  - getObjectDescription (catalog/objectaddress.c:2918)
  - get_rte_attribute_name (parser/parse_relation.c:3272)
  - pg_get_triggerdef_worker (utils/adt/ruleutils.c:971)
  - errtablecol (utils/cache/relcache.c:5983)

## Notes and Other Information
- Returns a palloc'ed string that must be freed by the caller
- Uses the ATTNUM system cache for efficient lookups
- Part of the PostgreSQL attribute cache subsystem in lsyscache.c
- Essential function for translating internal attribute numbers to user-visible column names
- Commonly used in error reporting, rule deparsing, and object description functions
- The missing_ok parameter allows graceful handling of non-existent attributes in some contexts