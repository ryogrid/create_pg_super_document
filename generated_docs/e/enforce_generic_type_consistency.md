# enforce_generic_type_consistency

## Location
[src/backend/parser/parse_coerce.c:2133-2876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L2133-L2876)

## Overview
Enforces type consistency rules for polymorphic functions and deduces actual argument and result types from polymorphic pseudotypes.

## Definition

```c
enum = (rettype == ANYENUMOID);
```
## Detailed Description
This function ensures that polymorphic functions are legally callable by enforcing consistency rules among polymorphic arguments and deducing concrete types. It handles two families of polymorphic types:

**Family-1 polymorphic types** (ANYELEMENT, ANYARRAY, ANYRANGE, etc.):
- All arguments of the same polymorphic type must resolve to the same concrete type
- Element types must be consistent across ANYELEMENT, ANYARRAY, and ANYRANGE arguments
- Special constraints for ANYENUM (must be enum type) and ANYNONARRAY (must not be array)

**Family-2 polymorphic types** (ANYCOMPATIBLE family):
- Arguments are resolved to a common supertype that all can be cast to
- Includes ANYCOMPATIBLE, ANYCOMPATIBLEARRAY, ANYCOMPATIBLERANGE, etc.
- Range and multirange types must have exact subtype matching

The function also handles UNKNOWN input literals by deducing their appropriate types and updates the declared_arg_types array accordingly for proper coercion.

## Parameters / Member Variables
- : Array of actual argument type OIDs passed to the function
- : Array of declared argument type OIDs (may be modified for UNKNOWN inputs)
- : Number of function arguments
- : Declared return type OID of the function
- : Whether polymorphic types are allowed in actual arguments and return type

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md)
  - [get_element_type](../g/get_element_type.md)
  - [get_range_subtype](../g/get_range_subtype.md)
  - [get_multirange_range](../g/get_multirange_range.md)
  - [get_range_multirange](../g/get_range_multirange.md)
  - [get_array_type](../g/get_array_type.md)
  - [select_common_type_from_oids](../s/select_common_type_from_oids.md)
  - [verify_common_type_from_oids](../v/verify_common_type_from_oids.md)
  - IsPolymorphicTypeFamily1
  - type_is_array_domain
  - [type_is_enum](../t/type_is_enum.md)
  - FUNC_MAX_ARGS
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [lookup_agg_function](../l/lookup_agg_function.md)
  - [make_op](../m/make_op.md)
  - [make_scalar_array_op](../m/make_scalar_array_op.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2133-2876
- Critical for PostgreSQL's polymorphic type system functionality
- Handles complex type resolution scenarios including domain flattening
- Special handling for pg_statistic columns that appear as anyarray
- Extensive error checking with detailed error messages for type mismatches
- Returns the resolved return type OID or original rettype if no polymorphic arguments

## Simplified Source

```c
Oid enforce_generic_type_consistency(const Oid *actual_arg_types,
                                    Oid *declared_arg_types,
                                    int nargs,
                                    Oid rettype,
                                    bool allow_poly)
{
    // Track polymorphic type families
    bool have_poly_anycompatible = false;
    bool have_poly_unknowns = false;

    // Family-1 polymorphic types (ANYELEMENT, ANYARRAY, ANYRANGE, etc.)
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid range_typeid = InvalidOid;
    Oid multirange_typeid = InvalidOid;

    // Family-2 polymorphic types (ANYCOMPATIBLE family)
    Oid anycompatible_typeid = InvalidOid;
    Oid anycompatible_array_typeid = InvalidOid;
    Oid anycompatible_range_typeid = InvalidOid;

    // Constraint flags from return type
    bool have_anynonarray = (rettype == ANYNONARRAYOID);
    bool have_anyenum = (rettype == ANYENUMOID);

    int n_poly_args = 0;
    int n_anycompatible_args = 0;
    Oid anycompatible_actual_types[FUNC_MAX_ARGS];

    // Phase 1: Analyze arguments and collect type information
    for (int j = 0; j < nargs; j++) {
        Oid decl_type = declared_arg_types[j];
        Oid actual_type = actual_arg_types[j];

        if (decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID) {
            // Handle family-1 element types
            n_poly_args++;
            if (decl_type == ANYNONARRAYOID) have_anynonarray = true;
            else if (decl_type == ANYENUMOID) have_anyenum = true;

            if (actual_type == UNKNOWNOID) {
                have_poly_unknowns = true;
                continue;
            }

            // Ensure consistency across ANYELEMENT arguments
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
                ereport(ERROR, "arguments declared \"anyelement\" are not all alike");
            elem_typeid = actual_type;
        }
        else if (decl_type == ANYARRAYOID) {
            // Handle family-1 array types
            n_poly_args++;
            if (actual_type == UNKNOWNOID) {
                have_poly_unknowns = true;
                continue;
            }
            actual_type = getBaseType(actual_type);  // flatten domains

            if (OidIsValid(array_typeid) && actual_type != array_typeid)
                ereport(ERROR, "arguments declared \"anyarray\" are not all alike");
            array_typeid = actual_type;
        }
        else if (decl_type == ANYRANGEOID || decl_type == ANYMULTIRANGEOID) {
            // Handle family-1 range types
            n_poly_args++;
            if (actual_type == UNKNOWNOID) {
                have_poly_unknowns = true;
                continue;
            }
            actual_type = getBaseType(actual_type);

            if (decl_type == ANYRANGEOID) {
                if (OidIsValid(range_typeid) && actual_type != range_typeid)
                    ereport(ERROR, "arguments declared \"anyrange\" are not all alike");
                range_typeid = actual_type;
            } else {
                if (OidIsValid(multirange_typeid) && actual_type != multirange_typeid)
                    ereport(ERROR, "arguments declared \"anymultirange\" are not all alike");
                multirange_typeid = actual_type;
            }
        }
        else if (decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID) {
            // Handle family-2 base types
            have_poly_anycompatible = true;
            if (actual_type != UNKNOWNOID && (!allow_poly || decl_type != actual_type))
                anycompatible_actual_types[n_anycompatible_args++] = actual_type;
        }
        else if (decl_type == ANYCOMPATIBLEARRAYOID) {
            // Handle family-2 array types
            have_poly_anycompatible = true;
            if (actual_type != UNKNOWNOID && (!allow_poly || decl_type != actual_type)) {
                actual_type = getBaseType(actual_type);
                Oid elem_type = get_element_type(actual_type);
                if (!OidIsValid(elem_type))
                    ereport(ERROR, "argument is not an array but type expected");
                anycompatible_actual_types[n_anycompatible_args++] = elem_type;
            }
        }
        // Similar handling for ANYCOMPATIBLERANGE and ANYCOMPATIBLEMULTIRANGE...
    }

    // Fast path: no polymorphic arguments
    if (n_poly_args == 0 && !have_poly_anycompatible)
        return rettype;

    // Phase 2: Resolve family-1 polymorphic types
    if (n_poly_args) {
        // Cross-validate array/element consistency
        if (OidIsValid(array_typeid)) {
            Oid array_elem = get_element_type(array_typeid);
            if (OidIsValid(elem_typeid) && array_elem != elem_typeid)
                ereport(ERROR, "anyarray not consistent with anyelement");
            if (!OidIsValid(elem_typeid))
                elem_typeid = array_elem;
        }

        // Cross-validate range/element consistency
        if (OidIsValid(range_typeid)) {
            Oid range_elem = get_range_subtype(range_typeid);
            if (OidIsValid(elem_typeid) && range_elem != elem_typeid)
                ereport(ERROR, "anyrange not consistent with anyelement");
            if (!OidIsValid(elem_typeid))
                elem_typeid = range_elem;
        }

        // Handle multirange/range consistency
        if (OidIsValid(multirange_typeid)) {
            Oid multirange_range = get_multirange_range(multirange_typeid);
            if (OidIsValid(range_typeid) && multirange_range != range_typeid)
                ereport(ERROR, "anymultirange not consistent with anyrange");
            if (!OidIsValid(range_typeid))
                range_typeid = multirange_range;
        }

        // Ensure we have a base element type
        if (!OidIsValid(elem_typeid)) {
            if (allow_poly) {
                elem_typeid = ANYELEMENTOID;
                array_typeid = ANYARRAYOID;
                range_typeid = ANYRANGEOID;
            } else {
                ereport(ERROR, "could not determine polymorphic type");
            }
        }

        // Apply constraints
        if (have_anynonarray && type_is_array_domain(elem_typeid))
            ereport(ERROR, "type matched to anynonarray is an array type");
        if (have_anyenum && !type_is_enum(elem_typeid))
            ereport(ERROR, "type matched to anyenum is not an enum type");
    }

    // Phase 3: Resolve family-2 polymorphic types
    if (have_poly_anycompatible) {
        if (n_anycompatible_args > 0) {
            // Find common supertype for all ANYCOMPATIBLE arguments
            anycompatible_typeid = select_common_type_from_oids(n_anycompatible_args,
                                                              anycompatible_actual_types,
                                                              false);
            if (!verify_common_type_from_oids(anycompatible_typeid,
                                            n_anycompatible_args,
                                            anycompatible_actual_types))
                ereport(ERROR, "arguments of anycompatible family cannot be cast to a common type");

            // Derive array type if needed
            if (anycompatible_array_typeid == InvalidOid) {
                anycompatible_array_typeid = get_array_type(anycompatible_typeid);
                if (!OidIsValid(anycompatible_array_typeid))
                    ereport(ERROR, "could not find array type");
            }
        } else {
            // Default to TEXT for unknown ANYCOMPATIBLE inputs
            anycompatible_typeid = allow_poly ? ANYCOMPATIBLEOID : TEXTOID;
            anycompatible_array_typeid = allow_poly ? ANYCOMPATIBLEARRAYOID : TEXTARRAYOID;
        }

        // Update declared types for family-2 polymorphic arguments
        for (int j = 0; j < nargs; j++) {
            Oid decl_type = declared_arg_types[j];
            if (decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID)
                declared_arg_types[j] = anycompatible_typeid;
            else if (decl_type == ANYCOMPATIBLEARRAYOID)
                declared_arg_types[j] = anycompatible_array_typeid;
        }
    }

    // Phase 4: Handle UNKNOWN inputs for family-1 types
    if (have_poly_unknowns) {
        for (int j = 0; j < nargs; j++) {
            if (actual_arg_types[j] != UNKNOWNOID)
                continue;

            Oid decl_type = declared_arg_types[j];
            if (decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID)
                declared_arg_types[j] = elem_typeid;
            else if (decl_type == ANYARRAYOID) {
                if (!OidIsValid(array_typeid))
                    array_typeid = get_array_type(elem_typeid);
                declared_arg_types[j] = array_typeid;
            }
            else if (decl_type == ANYRANGEOID)
                declared_arg_types[j] = range_typeid;
            else if (decl_type == ANYMULTIRANGEOID)
                declared_arg_types[j] = multirange_typeid;
        }
    }

    // Phase 5: Determine return type
    if (rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID)
        return elem_typeid;
    if (rettype == ANYARRAYOID) {
        if (!OidIsValid(array_typeid))
            array_typeid = get_array_type(elem_typeid);
        return array_typeid;
    }
    if (rettype == ANYRANGEOID)
        return range_typeid;
    if (rettype == ANYMULTIRANGEOID)
        return multirange_typeid;
    if (rettype == ANYCOMPATIBLEOID || rettype == ANYCOMPATIBLENONARRAYOID)
        return anycompatible_typeid;
    if (rettype == ANYCOMPATIBLEARRAYOID)
        return anycompatible_array_typeid;

    // Return original type if not polymorphic
    return rettype;
}
```