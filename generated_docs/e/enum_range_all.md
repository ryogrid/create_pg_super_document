# enum_range_all

## Location
[src/backend/utils/adt/enum.c:527-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L527-L546)

## Overview
A PostgreSQL built-in function that returns an array containing all values of an enum type in their sort order.

## Definition
```c
Datum enum_range_all(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_range_all` function implements the single-argument variant of the `enum_range` SQL function. It returns an array containing all enum values for the specified enum type, ordered according to the enum's sort order. The function determines the enum type from the expression tree and passes `InvalidOid` for both lower and upper bounds to `enum_range_internal`, which interprets this as "return all values".

Like other enum functions in PostgreSQL, the actual argument value is not examined - the enum type is derived from the calling expression tree, allowing the argument to be NULL while still determining the correct enum type to process.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the enum argument (value not examined, only type)

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [enum_range_internal](enum_range_internal.md)
  - ereport
  - PG_RETURN_ARRAYTYPE_P
- Called from:
  - SQL queries using enum_range(any_enum_value) function

## Notes and Other Information
- This is the single-argument variant of enum_range, complementing the two-argument `enum_range_bounds` function
- The argument value is not examined; only the type information is used
- Returns all enum values by passing InvalidOid bounds to `enum_range_internal`
- Returns an array of enum values, not individual values
- Raises an error if the enum type cannot be determined from the calling context
- The actual range generation logic is implemented in the shared `enum_range_internal` function
- Equivalent to calling enum_range_bounds with NULL bounds for both parameters

## Simplified Source

```c
Datum
enum_range_all(PG_FUNCTION_ARGS)
{
    // Determine enum type from function call context
    // (argument value is ignored, only type matters)
    Oid enumtypoid = get_fn_expr_argtype(fcinfo->flinfo, 0);
    if (enumtypoid == InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("could not determine actual enum type")));
    }

    // Return all enum values (InvalidOid bounds = no bounds)
    PG_RETURN_ARRAYTYPE_P(enum_range_internal(enumtypoid,
                                              InvalidOid, InvalidOid));
}
```

**Simplified Logic**: This function returns an array containing all values of an enum type. It determines the enum type from the function call context (not the argument value) and calls `enum_range_internal` with no bounds to get all enum values.