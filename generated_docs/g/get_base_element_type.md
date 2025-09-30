# get_base_element_type

## Location
[src/backend/utils/cache/lsyscache.c:2832-2873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2832-L2873)

## Overview
Retrieves the element type of an array by looking "through" any domain layers to find the underlying base array type, providing efficient domain-aware element type resolution without extra cache lookups.

## Definition
```c
Oid get_base_element_type(Oid typid)
```

## Detailed Description
This function provides domain-aware element type resolution by traversing through domain type layers to find the underlying base array type and then returning its element type. It is functionally equivalent to calling `get_element_type(getBaseType(typid))` but optimizes performance by avoiding an extra cache lookup.

The function operates by iterating through a potentially nested stack of domain types, following the `typbasetype` chain until it reaches a non-domain type. Once it finds the base type, it checks if it's a "true" array type using `IsTrueArrayType()`. If so, it returns the `typelem` field, which contains the OID of the array's element type. If the base type is not an array or if any lookup fails, it returns `InvalidOid`.

This function is particularly important in PostgreSQL's type system because domains can be layered on top of array types, and many operations need to understand the ultimate element type regardless of how many domain layers exist.

## Parameters / Member Variables
- `typid`: The OID of the type (potentially a domain over an array) for which to find the base element type

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - Form_pg_type
  - TYPTYPE_DOMAIN
  - IsTrueArrayType
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md)
  - [CreateFunction](../C/CreateFunction.md)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [make_scalar_array_op](../m/make_scalar_array_op.md)
  - [arraycontsel](../a/arraycontsel.md)
  - [array_typanalyze](../a/array_typanalyze.md)
  - type_is_array_domain

## Notes and Other Information
- Returns `InvalidOid` for non-array types or invalid input, silently handling bogus input like `get_element_type`
- Loops through domain layers by following the `typbasetype` field until reaching a non-domain type
- Performance optimization: avoids the extra cache lookup that would be required by calling `get_element_type(getBaseType(typid))`
- Does not provide information about the typmod of the array
- The test `IsTrueArrayType(typTup)` must match the logic used in `get_element_type` for consistency
- Essential for handling domains over array types, which are common in PostgreSQL applications

## Simplified Source

```c
Oid get_base_element_type(Oid typid) {
    // Loop through domain layers to find the base type
    for (;;) {
        HeapTuple tup;
        Form_pg_type typTup;

        // Look up the type in the system catalog
        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if (!HeapTupleIsValid(tup))
            break;

        typTup = (Form_pg_type) GETSTRUCT(tup);

        // If we've reached a non-domain type, check if it's an array
        if (typTup->typtype != TYPTYPE_DOMAIN) {
            Oid result;

            // Return element type if it's a true array, InvalidOid otherwise
            if (IsTrueArrayType(typTup))
                result = typTup->typelem;
            else
                result = InvalidOid;

            ReleaseSysCache(tup);
            return result;
        }

        // Continue traversing through domain to its base type
        typid = typTup->typbasetype;
        ReleaseSysCache(tup);
    }

    // Return InvalidOid for invalid input
    return InvalidOid;
}
```