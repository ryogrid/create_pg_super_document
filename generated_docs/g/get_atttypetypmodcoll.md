# get_atttypetypmodcoll

## Location
[src/backend/utils/cache/lsyscache.c:943-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L943-L969)

## Overview
Retrieves the complete type information (type OID, type modifier, and collation OID) for an attribute in a single efficient cache lookup operation.

## Definition
```c
void get_atttypetypmodcoll(Oid relid, AttrNumber attnum,
                          Oid *typid, int32 *typmod, Oid *collid)
```

## Detailed Description
This function performs a single system cache lookup to retrieve three related pieces of type information from the pg_attribute catalog: the type OID (atttypid), type modifier (atttypmod), and collation OID (attcollation). This is more efficient than making separate calls to get each piece of information individually. Unlike similar functions like get_atttype, this function always throws an error if the attribute cannot be found, ensuring that all output parameters are populated with valid data.

## Parameters / Member Variables
- `relid`: Object identifier of the relation (table/view/etc.) containing the attribute
- `attnum`: Attribute number (column number) within the relation
- `typid`: Output parameter - receives the type OID (atttypid) of the attribute
- `typmod`: Output parameter - receives the type modifier (atttypmod) of the attribute
- `collid`: Output parameter - receives the collation OID (attcollation) of the attribute

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) - performs the system cache lookup using ATTNUM cache
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) - converts relation OID to Datum format
  - [Int16GetDatum](../I/Int16GetDatum.md) - converts attribute number to Datum format
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md) - releases the system cache entry
  - elog - logs error messages when attribute is not found

- Called from (representative examples):
  - [transformAssignmentIndirection](../t/transformAssignmentIndirection.md) (parser/parse_target.c:799)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md) (utils/adt/ruleutils.c:1410)
  - [pg_get_partkeydef_worker](../p/pg_get_partkeydef_worker.md) (utils/adt/ruleutils.c:2023)

## Notes and Other Information
- More efficient than calling separate functions for type, typmod, and collation information
- Always throws an error if the attribute is not found, unlike some other attribute functions
- Essential for operations that need complete type information for type checking and coercion
- The type modifier (typmod) contains additional type-specific information like precision for numeric types
- The collation OID specifies the collation rules for text-based types
- Used primarily in parser operations, rule deparsing, and type system operations
- All three output parameters are guaranteed to be populated if the function returns successfully
- Part of PostgreSQL's type system infrastructure for efficient type information retrieval

## Simplified Source

```c
void get_atttypetypmodcoll(Oid relid, AttrNumber attnum,
                         Oid *typid, int32 *typmod, Oid *collid) {
    HeapTuple tp;
    Form_pg_attribute att_tup;

    // Lookup attribute in system cache
    tp = SearchSysCache2(ATTNUM,
                        ObjectIdGetDatum(relid),
                        Int16GetDatum(attnum));

    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for attribute %d of relation %u", attnum, relid);

    // Extract attribute information
    att_tup = (Form_pg_attribute) GETSTRUCT(tp);

    // Return type information through output parameters
    *typid = att_tup->atttypid;      // Type OID
    *typmod = att_tup->atttypmod;    // Type modifier
    *collid = att_tup->attcollation; // Collation OID

    ReleaseSysCache(tp);
}
```