# format_procedure_parts

## Location
[src/backend/utils/adt/regproc.c:398-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L398-L434)

## Overview
Outputs an objname/objargs representation for a procedure with the given OID, which can be used to feed get_object_address for object identification and manipulation.

## Definition

```c
void
format_procedure_parts(Oid procedure_oid, List **objnames, List **objargs,
					   bool missing_ok)
```
## Detailed Description
This function retrieves procedure information from the system catalog and formats it into a standardized representation consisting of object names and argument types. It looks up the procedure in pg_proc using the provided OID, extracts the procedure's namespace and name, and builds a list of qualified argument type names. The function is designed to work with PostgreSQL's object addressing system, providing a way to represent procedures in a format that can be consumed by other object management functions.

The function handles missing procedures gracefully when the missing_ok parameter is true, otherwise it throws an error if the procedure cannot be found.

## Parameters / Member Variables
- `procedure_oid`: The OID of the procedure to format
- `**objnames`: Output parameter - pointer to a List that will contain the namespace and procedure name
- `**objargs`: Output parameter - pointer to a List that will contain the qualified argument type names
- `missing_ok`: If true, the function returns silently when the procedure is not found; if false, an error is thrown
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - Form_pg_proc
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md)
  - list_make2
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [lappend](../l/lappend.md)
  - [format_type_be_qualified](format_type_be_qualified.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (src/backend/catalog/objectaddress.c:4822)

## Notes and Other Information
- The function is part of PostgreSQL's regproc type handling system
- It builds two output lists: objnames contains [namespace, procedure_name] and objargs contains qualified type names for each argument
- The function properly manages system cache resources by releasing the heap tuple after use
- This function is essential for object identity operations and is used in dependency tracking and object addressing within PostgreSQL

## Simplified Source

```c
void
format_procedure_parts(Oid procedure_oid, List **objnames, List **objargs, bool missing_ok)
{
    HeapTuple proctup;
    Form_pg_proc procform;
    int nargs, i;

    // Look up procedure in system catalog
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedure_oid));

    if (!HeapTupleIsValid(proctup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for procedure with OID %u", procedure_oid);
        return;
    }

    // Extract procedure information
    procform = (Form_pg_proc) GETSTRUCT(proctup);
    nargs = procform->pronargs;

    // Build names list: [namespace, procedure_name]
    *objnames = list_make2(get_namespace_name_or_temp(procform->pronamespace),
                          pstrdup(NameStr(procform->proname)));

    // Build argument types list
    *objargs = NIL;
    for (i = 0; i < nargs; i++) {
        Oid argtype = procform->proargtypes.values[i];
        *objargs = lappend(*objargs, format_type_be_qualified(argtype));
    }

    ReleaseSysCache(proctup);
}
```