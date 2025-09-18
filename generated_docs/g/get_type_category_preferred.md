# get_type_category_preferred

## Location
src/backend/utils/cache/lsyscache.c: 2710 - 2730

## Overview
A system cache utility function that retrieves both the category and preferred-type status for a given PostgreSQL type OID.

## Definition
```c
void get_type_category_preferred(Oid typid, char *typcategory, bool *typispreferred)
```

## Detailed Description
This function performs a system catalog lookup to fetch two important type attributes from the pg_type system catalog: the type category and whether the type is marked as preferred within its category. Type categories group related types together (e.g., numeric types, string types), and preferred types are used by the type resolution system to make decisions when multiple candidate types are available. The function throws an error if the type OID is not found in the system catalog.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to look up
- `typcategory`: Output parameter that receives the type's category character
- `typispreferred`: Output parameter that receives whether the type is preferred in its category

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_type
- Called from (representative examples):
  - select_common_type
  - select_common_type_from_oids
  - TypeCategory
  - IsPreferredType
  - transformJsonValueExpr
  - func_select_candidate

## Notes and Other Information
This function is a core component of PostgreSQL's type system infrastructure. It's heavily used in type resolution algorithms, particularly in contexts where the parser needs to determine which type to use when multiple options are available (such as in function overload resolution or UNION operations). The function accesses the system cache for efficiency, as type information is frequently queried during query planning and execution.