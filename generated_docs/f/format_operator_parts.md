# format_operator_parts

## Location
[src/backend/utils/adt/regproc.c:806-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L806-L838)

## Overview
Decomposes an operator OID into its constituent parts (schema name, operator name, and argument types) returned as separate lists for use in object identity operations.

## Definition

```c
void
format_operator_parts(Oid operator_oid, List **objnames, List **objargs,
					  bool missing_ok)
```
## Detailed Description
The  function breaks down an operator into its structural components rather than returning a formatted string. This function is specifically designed for use by PostgreSQL's object identity system, which needs to handle object names and arguments as separate entities.

The function queries the pg_operator system catalog and populates two output lists:  containing the schema name and operator name, and  containing the formatted argument type names. This decomposed format is particularly useful for operations that need to manipulate or compare individual components of operator identities.

## Parameters / Member Variables
- `operator_oid`: The OID of the operator to decompose
- `**objnames`: Output parameter - pointer to a List that will contain schema name and operator name
- `**objargs`: Output parameter - pointer to a List that will contain formatted argument type names
- `missing_ok`: If true, function returns silently for invalid operator OIDs; if false, throws an error
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md)
  - list_make2
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [lappend](../l/lappend.md)
  - [format_type_be_qualified](format_type_be_qualified.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (src/backend/catalog/objectaddress.c:5045)

## Notes and Other Information
- This function is primarily used by PostgreSQL's object addressing system
- Returns void - results are provided through output parameters (objnames and objargs)
- The objnames list contains exactly two elements: [schema_name, operator_name]
- The objargs list contains 1-2 elements depending on whether the operator is unary or binary
- Uses qualified type names for arguments to ensure uniqueness
- The missing_ok parameter allows graceful handling of invalid operator OIDs
- Does not allocate return strings - caller is responsible for list memory management
- Located in src/backend/utils/adt/regproc.c:806-838

## Simplified Source

```c
void
format_operator_parts(Oid operator_oid, List **objnames, List **objargs, bool missing_ok)
{
    HeapTuple opertup;
    Form_pg_operator oprForm;

    // Look up operator in system catalog
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator_oid));
    if (!HeapTupleIsValid(opertup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for operator with OID %u", operator_oid);
        return;
    }

    // Extract operator information
    oprForm = (Form_pg_operator) GETSTRUCT(opertup);

    // Build names list: [schema_name, operator_name]
    *objnames = list_make2(get_namespace_name_or_temp(oprForm->oprnamespace),
                          pstrdup(NameStr(oprForm->oprname)));

    // Build argument types list
    *objargs = NIL;
    if (oprForm->oprleft)
        *objargs = lappend(*objargs, format_type_be_qualified(oprForm->oprleft));
    if (oprForm->oprright)
        *objargs = lappend(*objargs, format_type_be_qualified(oprForm->oprright));

    ReleaseSysCache(opertup);
}
```