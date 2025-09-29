# get_collation

## Location
[src/backend/parser/parse_utilcmd.c:1992-2025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L1992-L2025)

## Overview
Fetches the qualified name of a collation for use in SQL statement generation, returning NULL if the collation is invalid or matches the data type's default collation.

## Definition
static List *get_collation(Oid collation, Oid actual_datatype)

## Detailed Description
This function retrieves the fully qualified name (schema.name) of a collation given its OID, but only if it differs from the default collation for the specified data type. It checks if the provided collation OID is valid and whether it matches the default collation for the actual data type. If the collation is either invalid or the default, it returns NIL to indicate that no explicit collation specification is needed. Otherwise, it looks up the collation in pg_collation, extracts the namespace and collation names, and returns them as a two-element list suitable for constructing SQL statements.

## Parameters / Member Variables
- `collation`: OID of the collation to look up and qualify
- `actual_datatype`: OID of the data type to check against for default collation comparison

## Dependencies
- Functions called/Symbols referenced:
  - [get_typcollation](get_typcollation.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [get_namespace_name](get_namespace_name.md)
  - [makeString](../m/makeString.md)
  - list_make2
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [generateClonedIndexStmt](generateClonedIndexStmt.md)

## Notes and Other Information
- Returns NIL (empty list) when collation specification is unnecessary
- Always schema-qualifies collation names for simplicity and clarity
- Used primarily in index and table cloning operations where collation needs to be preserved
- Part of the utility command parsing infrastructure for DDL statement generation
- Helps avoid redundant collation specifications when the default would suffice
- Essential for maintaining collation semantics when copying table structures

## Simplified Source

```c
static List *get_collation(Oid collation, Oid actual_datatype)
{
    // Return NIL for invalid collation
    if (!OidIsValid(collation))
        return NIL;

    // Return NIL if this is the default collation for the data type
    if (collation == get_typcollation(actual_datatype))
        return NIL;

    // Look up the collation in system cache
    HeapTuple ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
    if (!HeapTupleIsValid(ht_coll))
        elog(ERROR, "cache lookup failed for collation %u", collation);

    // Extract collation record
    Form_pg_collation coll_rec = (Form_pg_collation) GETSTRUCT(ht_coll);

    // Get schema and collation names
    char *nsp_name = get_namespace_name(coll_rec->collnamespace);
    char *coll_name = pstrdup(NameStr(coll_rec->collname));

    // Build qualified name list: schema.name
    List *result = list_make2(makeString(nsp_name), makeString(coll_name));

    ReleaseSysCache(ht_coll);
    return result;
}
```