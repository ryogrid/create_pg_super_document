# format_procedure_parts

## Location
src/backend/utils/adt/regproc.c: 398 - 434

## Overview
Outputs an objname/objargs representation for a procedure with the given OID, which can be used to feed get_object_address for object identification and manipulation.

## Definition


## Detailed Description
This function retrieves procedure information from the system catalog and formats it into a standardized representation consisting of object names and argument types. It looks up the procedure in pg_proc using the provided OID, extracts the procedure's namespace and name, and builds a list of qualified argument type names. The function is designed to work with PostgreSQL's object addressing system, providing a way to represent procedures in a format that can be consumed by other object management functions.

The function handles missing procedures gracefully when the missing_ok parameter is true, otherwise it throws an error if the procedure cannot be found.

## Parameters / Member Variables
- : The OID of the procedure to format
- : Output parameter - pointer to a List that will contain the namespace and procedure name
- : Output parameter - pointer to a List that will contain the qualified argument type names
- : If true, the function returns silently when the procedure is not found; if false, an error is thrown

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
  - lappend
  - [format_type_be_qualified](format_type_be_qualified.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (src/backend/catalog/objectaddress.c:4822)

## Notes and Other Information
- The function is part of PostgreSQL's regproc type handling system
- It builds two output lists: objnames contains [namespace, procedure_name] and objargs contains qualified type names for each argument
- The function properly manages system cache resources by releasing the heap tuple after use
- This function is essential for object identity operations and is used in dependency tracking and object addressing within PostgreSQL