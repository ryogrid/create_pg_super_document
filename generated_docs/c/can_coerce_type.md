# can_coerce_type

## Location
[src/backend/parser/parse_coerce.c:556-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L556-L675)

## Overview
Determines whether a set of input types can be coerced to corresponding target types within a given coercion context.

## Definition

```c
bool
can_coerce_type(int nargs, const Oid *input_typeids, const Oid *target_typeids,
				CoercionContext ccontext)
```
## Detailed Description
This function serves as the feasibility checker for type coercion operations, validating whether type conversions are possible before attempting them. It evaluates each input-target type pair according to PostgreSQL's coercion rules:

1. **Identity Check**: Same types always coerce successfully
2. **ANY Type**: ANY pseudotype accepts any input
3. **Polymorphic Types**: Polymorphic pseudotypes are accepted with additional consistency checking
4. **Unknown Constants**: UNKNOWN type can coerce to any target type
5. **Cast System**: Consults pg_cast catalog for explicit coercion functions and binary compatibility
6. **Record Coercion**: Handles RECORD to/from complex types
7. **Array Coercion**: Manages complex array type conversions
8. **Inheritance**: Supports subclass to superclass coercion via type inheritance

The function performs polymorphic type consistency checking when generic types are involved, ensuring that all polymorphic parameters resolve to compatible actual types.

## Parameters / Member Variables
- `nargs`: Number of argument types to check for coercion
- `*input_typeids`: Array of input type OIDs to be coerced from
- `*target_typeids`: Array of target type OIDs to be coerced to
- `ccontext`: Coercion context (CAST, assignment, implicit) determining available casts
## Dependencies
- Functions called/Symbols referenced:
  - [find_coercion_pathway](../f/find_coercion_pathway.md)
  - IsPolymorphicType
  - [check_generic_type_consistency](check_generic_type_consistency.md)
  - [typeInheritsFrom](../t/typeInheritsFrom.md)
  - [typeIsOfTypedTable](../t/typeIsOfTypedTable.md)
  - [is_complex_array](../i/is_complex_array.md)
  - ISCOMPLEX
  - CoercionContext (enum)
  - [CoercionPathType](../C/CoercionPathType.md) (enum)
- Called from (representative examples):
  - [coerce_to_target_type](coerce_to_target_type.md)
  - [select_common_type](../s/select_common_type.md)
  - [func_match_argtypes](../f/func_match_argtypes.md)
  - [func_select_candidate](../f/func_select_candidate.md)
  - [verify_common_type](../v/verify_common_type.md)

## Notes and Other Information
- Returns false immediately upon finding any non-coercible type pair
- Performs polymorphic type consistency validation as a final step when generic types are present
- Does not perform actual coercion - only validates feasibility
- Essential prerequisite function called before  operations
- Supports both single and multi-argument type checking scenarios
- Contains provisions for record array coercion (currently disabled with NOT_USED)
- Located in src/backend/parser/parse_coerce.c:556-675

## Simplified Source

```c
bool
can_coerce_type(int nargs, const Oid *input_typeids, const Oid *target_typeids,
                CoercionContext ccontext)
{
    bool have_generics = false;

    // Check each input-target type pair
    for (int i = 0; i < nargs; i++) {
        Oid inputTypeId = input_typeids[i];
        Oid targetTypeId = target_typeids[i];

        // Same type always works
        if (inputTypeId == targetTypeId)
            continue;

        // ANY accepts everything
        if (targetTypeId == ANYOID)
            continue;

        // Handle polymorphic types (need consistency check later)
        if (IsPolymorphicType(targetTypeId)) {
            have_generics = true;
            continue;
        }

        // Unknown constants can convert to anything
        if (inputTypeId == UNKNOWNOID)
            continue;

        // Check for explicit cast pathway
        CoercionPathType pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
        if (pathtype != COERCION_PATH_NONE)
            continue;

        // Special cases for record types
        if ((inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) ||
            (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)))
            continue;

        // Check inheritance relationships
        if (typeInheritsFrom(inputTypeId, targetTypeId) ||
            typeIsOfTypedTable(inputTypeId, targetTypeId))
            continue;

        // No coercion possible for this pair
        return false;
    }

    // Final check for polymorphic type consistency
    if (have_generics) {
        if (!check_generic_type_consistency(input_typeids, target_typeids, nargs))
            return false;
    }

    return true;
}
```