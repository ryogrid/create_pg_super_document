# get_opclass_oid

## Location
[src/backend/commands/opclasscmds.c:220-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L220-L242)

## Overview
get_opclass_oid is a utility function that finds an operator class OID by its possibly qualified name, serving as a convenient wrapper around OpClassCacheLookup.

## Definition
```c
Oid get_opclass_oid(Oid amID, List *opclassname, bool missing_ok)
```

## Detailed Description
This function provides a clean interface for retrieving operator class OIDs from the system catalog. It leverages the OpClassCacheLookup function to perform the actual catalog lookup, then extracts the OID from the returned tuple structure. The function handles proper memory management by releasing the syscache tuple after extracting the required information.

Similar to get_opfamily_oid, this function serves as a higher-level abstraction that simplifies the common task of converting operator class names to their corresponding OIDs. Operator class OIDs are frequently needed throughout PostgreSQL for various operations involving indexing, type operations, and query optimization.

## Parameters
- `amID`: The OID of the access method that the operator class belongs to
- `opclassname`: A list representing the qualified or unqualified name of the operator class to look up
- `missing_ok`: If true, returns InvalidOid when the operator class is not found; if false, allows the underlying function to raise an error

## Dependencies
- Functions called/Symbols referenced:
  - [OpClassCacheLookup](../O/OpClassCacheLookup.md)
  - Form_pg_opclass
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [get_object_address_opcf](get_object_address_opcf.md)
  - [findRangeSubOpclass](../f/findRangeSubOpclass.md)
  - [resolve_unique_index_expr](../r/resolve_unique_index_expr.md)

## Notes and Other Information
- This function is exported and can be called from other source files, as evidenced by its declaration in defrem.h
- Proper syscache memory management is implemented by calling ReleaseSysCache() after extracting the OID
- The function returns InvalidOid rather than throwing an error when missing_ok is true, making it suitable for optional lookups
- Used extensively throughout PostgreSQL for index creation, type handling, and query planning operations
- Follows the same pattern as get_opfamily_oid but operates on operator classes instead of operator families

## Simplified Source

```c
Oid
get_opclass_oid(Oid amID, List *opclassname, bool missing_ok)
{
    HeapTuple htup;
    Form_pg_opclass opcform;
    Oid opcID;

    // Look up operator class in cache
    htup = OpClassCacheLookup(amID, opclassname, missing_ok);
    if (!HeapTupleIsValid(htup))
        return InvalidOid;

    // Extract OID from tuple
    opcform = (Form_pg_opclass) GETSTRUCT(htup);
    opcID = opcform->oid;
    ReleaseSysCache(htup);

    return opcID;
}
```