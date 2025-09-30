# check_generic_type_consistency

## Location
[src/backend/parser/parse_coerce.c:1739-2132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1739-L2132)

## Overview
Validates that actual argument types are potentially compatible with a polymorphic function's declared argument types, enforcing PostgreSQL's complex polymorphic type consistency rules.

## Definition
```c
bool check_generic_type_consistency(const Oid *actual_arg_types, const Oid *declared_arg_types, int nargs)
```

## Detailed Description
This function implements PostgreSQL's comprehensive polymorphic type consistency checking system. It validates whether actual argument types can satisfy the constraints imposed by polymorphic pseudo-types like ANYELEMENT, ANYARRAY, ANYRANGE, ANYCOMPATIBLE, etc. The function processes multiple categories of polymorphic types: traditional ANY* types that require exact matching, and newer ANYCOMPATIBLE* types that allow implicit coercion to a common supertype. It handles complex scenarios involving arrays, ranges, multiranges, enums, and their element/subtype relationships, ensuring all constraints are satisfied before allowing function calls to proceed.

## Parameters / Member Variables
- `actual_arg_types`: Array of actual argument type OIDs being passed to the function
- `declared_arg_types`: Array of declared parameter type OIDs from the function signature
- `nargs`: Number of arguments to check (must be ≤ FUNC_MAX_ARGS)

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md) (flatten domain types to base types)
  - [get_element_type](../g/get_element_type.md) (extract array element types)
  - [get_range_subtype](../g/get_range_subtype.md) (extract range subtypes)
  - [get_multirange_range](../g/get_multirange_range.md) (extract multirange element types)
  - [select_common_type_from_oids](../s/select_common_type_from_oids.md) (find common supertype)
  - [verify_common_type_from_oids](../v/verify_common_type_from_oids.md) (verify coercion feasibility)
  - type_is_array_domain (check if type is array or domain over array)
  - [type_is_enum](../t/type_is_enum.md) (check if type is enum)
  - FUNC_MAX_ARGS (maximum function argument limit)
  - Various polymorphic type OID constants (ANYELEMENTOID, ANYARRAYOID, etc.)

- Called from (representative examples):
  - [can_coerce_type](can_coerce_type.md) (type coercion feasibility checking)

## Notes and Other Information
- Implements 10 detailed consistency rules covering all polymorphic type combinations
- Handles traditional polymorphic types (ANY*) requiring exact type matching
- Supports newer ANYCOMPATIBLE* types allowing implicit coercion to common supertypes
- Automatically flattens domain types to their base types for comparison
- Treats UNKNOWNOID (untyped literals) as compatible with any polymorphic type
- Returns false for constraint violations without raising errors (error reporting handled by callers)
- Essential for PostgreSQL's polymorphic function resolution system
- Supports complex type hierarchies including arrays, ranges, multiranges, and enums
- Located in src/backend/parser/parse_coerce.c:1739-2132

## Simplified Source

```c
bool
check_generic_type_consistency(const Oid *actual_arg_types,
                              const Oid *declared_arg_types,
                              int nargs)
{
    // Track resolved types for different polymorphic categories
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid range_typeid = InvalidOid;
    Oid multirange_typeid = InvalidOid;
    Oid anycompatible_range_typeid = InvalidOid;
    Oid anycompatible_range_typelem = InvalidOid;

    // Track constraint flags and compatible types
    bool have_anynonarray = false;
    bool have_anyenum = false;
    bool have_anycompatible_nonarray = false;
    int n_anycompatible_args = 0;
    Oid anycompatible_actual_types[FUNC_MAX_ARGS];

    // Process each argument to check polymorphic type consistency
    for (int j = 0; j < nargs; j++)
    {
        Oid decl_type = declared_arg_types[j];
        Oid actual_type = actual_arg_types[j];

        // Handle traditional ANY* types (require exact matching)
        if (decl_type == ANYELEMENTOID ||
            decl_type == ANYNONARRAYOID ||
            decl_type == ANYENUMOID)
        {
            // Track special constraints
            if (decl_type == ANYNONARRAYOID)
                have_anynonarray = true;
            else if (decl_type == ANYENUMOID)
                have_anyenum = true;

            // Skip unknown types, check consistency with resolved type
            if (actual_type == UNKNOWNOID)
                continue;
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
                return false;
            elem_typeid = actual_type;
        }
        else if (decl_type == ANYARRAYOID)
        {
            // Handle array types - must all be same array type
            if (actual_type == UNKNOWNOID)
                continue;
            actual_type = getBaseType(actual_type);  // flatten domains
            if (OidIsValid(array_typeid) && actual_type != array_typeid)
                return false;
            array_typeid = actual_type;
        }
        else if (decl_type == ANYRANGEOID)
        {
            // Handle range types - must all be same range type
            if (actual_type == UNKNOWNOID)
                continue;
            actual_type = getBaseType(actual_type);
            if (OidIsValid(range_typeid) && actual_type != range_typeid)
                return false;
            range_typeid = actual_type;
        }
        // Handle ANYCOMPATIBLE* types (allow coercion to common supertype)
        else if (decl_type == ANYCOMPATIBLEOID ||
                 decl_type == ANYCOMPATIBLENONARRAYOID)
        {
            if (decl_type == ANYCOMPATIBLENONARRAYOID)
                have_anycompatible_nonarray = true;
            if (actual_type != UNKNOWNOID)
                anycompatible_actual_types[n_anycompatible_args++] = actual_type;
        }
        // ... additional polymorphic type handling for arrays, ranges, etc.
    }

    // Validate consistency between resolved types
    // Check array element type matches element type
    if (OidIsValid(array_typeid) && array_typeid != ANYARRAYOID)
    {
        Oid array_typelem = get_element_type(array_typeid);
        if (!OidIsValid(array_typelem))
            return false;  // not actually an array

        if (!OidIsValid(elem_typeid))
            elem_typeid = array_typelem;
        else if (array_typelem != elem_typeid)
            return false;  // element types don't match
    }

    // Check range subtype matches element type
    if (OidIsValid(range_typeid))
    {
        Oid range_typelem = get_range_subtype(range_typeid);
        if (!OidIsValid(range_typelem))
            return false;  // not actually a range

        if (!OidIsValid(elem_typeid))
            elem_typeid = range_typelem;
        else if (range_typelem != elem_typeid)
            return false;  // subtypes don't match
    }

    // Apply special constraints
    if (have_anynonarray && type_is_array_domain(elem_typeid))
        return false;  // ANYNONARRAY requires non-array type

    if (have_anyenum && !type_is_enum(elem_typeid))
        return false;  // ANYENUM requires enum type

    // Handle ANYCOMPATIBLE family - find common supertype
    if (n_anycompatible_args > 0)
    {
        Oid anycompatible_typeid =
            select_common_type_from_oids(n_anycompatible_args,
                                        anycompatible_actual_types, true);

        if (!OidIsValid(anycompatible_typeid))
            return false;  // no common supertype exists

        // Verify all types can be coerced to the common type
        if (!verify_common_type_from_oids(anycompatible_typeid,
                                         n_anycompatible_args,
                                         anycompatible_actual_types))
            return false;

        // Apply ANYCOMPATIBLENONARRAY constraint
        if (have_anycompatible_nonarray &&
            type_is_array_domain(anycompatible_typeid))
            return false;
    }

    return true;  // All consistency checks passed
}
```