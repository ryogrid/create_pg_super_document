# get_element_type

## Location
[src/backend/utils/cache/lsyscache.c:2759-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2759-L2786)

## Overview
A system cache utility function that retrieves the element type OID for a given array type, returning InvalidOid for non-array types.

## Definition
```c
Oid get_element_type(Oid typid)
```

## Detailed Description
This function performs a system catalog lookup to determine the element type of an array type. It specifically checks if the given type is a "true" array type (one that has array_subscript_handler as its typsubscript function) and returns the typelem field which contains the OID of the array's element type. For non-array types or types that don't qualify as true arrays, it returns InvalidOid. This distinction is important because PostgreSQL allows some types to have typelem set without being true arrays.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to examine

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - IsTrueArrayType
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_type
  - InvalidOid
- Called from (representative examples):
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - [CheckAttributeType](../C/CheckAttributeType.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [CreateCast](../C/CreateCast.md)
  - [build_coercion_expression](../b/build_coercion_expression.md)
  - [transformArrayExpr](../t/transformArrayExpr.md)
  - [json_categorize_type](../j/json_categorize_type.md)
  - [get_promoted_array_type](get_promoted_array_type.md)

## Notes and Other Information
This function is crucial for array type handling throughout PostgreSQL. It's used in type coercion, function parameter validation, array operations, and many other contexts where the system needs to understand the relationship between array types and their elements. The function's strict checking for "true" array types helps maintain type safety by ensuring that only proper arrays are treated as such, even if other types might have some array-like characteristics.

## Simplified Source

```c
Oid get_element_type(Oid typid) {
    // Look up the type in system cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        Oid result;

        // Return element type only for true array types
        if (IsTrueArrayType(typtup)) {
            result = typtup->typelem;
        } else {
            result = InvalidOid;
        }

        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}
```