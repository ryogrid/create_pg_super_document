# lookup_agg_function

## Location
[src/backend/catalog/pg_aggregate.c:826-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_aggregate.c#L826-L914)

## Overview
lookup_agg_function is a utility function that validates and resolves function names for aggregate support functions, ensuring they meet the requirements for use in aggregate definitions.

## Definition

```c
static Oid
lookup_agg_function(List *fnName,
					int nargs,
					Oid *input_types,
					Oid variadicArgType,
					Oid *rettype)
```
## Detailed Description
lookup_agg_function serves as the common validation and resolution function for all aggregate support functions (transition functions, final functions, combine functions, etc.). It performs function lookup using the system catalog, handles polymorphic type resolution, ensures type consistency, validates that no runtime type coercion is required, and checks permissions.

The function uses func_get_detail to resolve the function name and handle polymorphic types, then performs additional validation specific to aggregate functions. It ensures the function doesn't return a set, handles VARIADIC ANY consistency, validates that no runtime type coercion will be needed, and verifies the caller has execute permissions on the function.

## Parameters / Member Variables
- `*fnName`: List representing the possibly schema-qualified function name to lookup
- `nargs`: Number of expected function arguments
- `*input_types`: Array of expected argument type OIDs (must not be modified)
- `variadicArgType`: OID of variadic argument type if any, InvalidOid otherwise
- `*rettype`: Pointer to store the resolved return type OID of the function
## Dependencies
- Functions called/Symbols referenced:
  - [func_get_detail](../f/func_get_detail.md): Core function lookup and polymorphic resolution
  - [func_signature_string](../f/func_signature_string.md): Creates function signature strings for error messages
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md): Ensures polymorphic type consistency
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md): Checks if types are binary compatible without coercion
  - [object_aclcheck](../o/object_aclcheck.md): Verifies execute permissions on the function
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission errors
  - [get_func_name](../g/get_func_name.md): Gets function name for error reporting

- Called from (representative examples):
  - [AggregateCreate](../A/AggregateCreate.md): Used 8 times to validate different aggregate support functions (transition, final, combine, serial/deserial, moving-aggregate functions)

## Notes and Other Information
This function is critical for aggregate validation because it ensures that all component functions can be called efficiently without runtime type coercion, which is essential for aggregate performance. The function is particularly careful about VARIADIC ANY consistency and polymorphic type resolution, as these are common sources of aggregate definition errors.

The function enforces that aggregate support functions cannot return sets, as this would be meaningless in the context of aggregation. It also validates that the caller has execute permissions on all referenced functions, ensuring proper security in aggregate definitions.

## Simplified Source

```c
static Oid
lookup_agg_function(List *fnName, int nargs, Oid *input_types,
                   Oid variadicArgType, Oid *rettype)
{
    Oid fnOid, *true_oid_array;
    bool retset;
    int nvargs;
    Oid vatype;

    // Look up function in catalogs and resolve polymorphic types
    FuncDetailCode fdresult = func_get_detail(fnName, NIL, NIL,
                                              nargs, input_types, false, false, false,
                                              &fnOid, rettype, &retset,
                                              &nvargs, &vatype, &true_oid_array, NULL);

    // Function must exist and be normal (not set-returning)
    if (fdresult != FUNCDETAIL_NORMAL || !OidIsValid(fnOid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("function %s does not exist",
                             func_signature_string(fnName, nargs, NIL, input_types))));

    if (retset)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("function %s returns a set",
                             func_signature_string(fnName, nargs, NIL, input_types))));

    // Check VARIADIC ANY consistency
    if (variadicArgType == ANYOID && vatype != ANYOID)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("function %s must accept VARIADIC ANY to be used in this aggregate",
                             func_signature_string(fnName, nargs, NIL, input_types))));

    // Enforce polymorphic type consistency
    *rettype = enforce_generic_type_consistency(input_types, true_oid_array,
                                               nargs, *rettype, true);

    // Ensure no runtime type coercion is needed
    for (int i = 0; i < nargs; i++) {
        if (!IsBinaryCoercible(input_types[i], true_oid_array[i]))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("function %s requires run-time type coercion",
                                 func_signature_string(fnName, nargs, NIL, true_oid_array))));
    }

    // Check execute permissions
    AclResult aclresult = object_aclcheck(ProcedureRelationId, fnOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fnOid));

    return fnOid;
}
```