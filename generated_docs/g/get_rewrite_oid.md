# get_rewrite_oid

## Location
[src/backend/rewrite/rewriteSupport.c:92-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSupport.c#L92-L116)

## Overview
Retrieves the OID of a rewrite rule given the relation ID and rule name, with optional error handling for missing rules.

## Definition
```c
Oid get_rewrite_oid(Oid relid, const char *rulename, bool missing_ok)
```

## Detailed Description
get_rewrite_oid is a utility function that looks up a specific rewrite rule by its name and owning relation, returning the rule's OID from the pg_rewrite system catalog. The function provides flexible error handling through the missing_ok parameter, allowing callers to either receive an InvalidOid for missing rules or have an error thrown.

The function searches the system cache using the RULERELNAME cache, which indexes rules by both relation ID and rule name for efficient lookup. When a rule is found, it extracts the OID from the pg_rewrite tuple and includes an assertion to verify that the relation ID matches the expected value.

This function is essential for PostgreSQL's object management system, particularly when dealing with rule-specific operations that require the rule's OID for further processing.

## Parameters / Member Variables
- `relid`: The OID of the relation that owns the rule
- `rulename`: The name of the rewrite rule to look up
- `missing_ok`: If false, throws an error when the rule is not found; if true, returns InvalidOid instead

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - HeapTupleIsValid
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [get_rel_name](get_rel_name.md)
  - GETSTRUCT
  - Assert
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Types used:
  - HeapTuple
  - Form_pg_rewrite
  - Oid
- Called from (representative examples):
  - [get_object_address_relobject](get_object_address_relobject.md)

## Notes and Other Information
- Uses the RULERELNAME system cache for efficient rule lookup by relation and name
- Returns InvalidOid when missing_ok is true and the rule doesn't exist
- Throws a detailed error message including both rule name and relation name when missing_ok is false
- Includes an assertion to verify data consistency between the search parameters and the found tuple
- The function properly releases the system cache tuple after extracting the needed information
- Essential for PostgreSQL's dependency tracking and object management systems
- Located in src/backend/rewrite/rewriteSupport.c:92-116