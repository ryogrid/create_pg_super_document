# resolve_anyarray_from_others

## Location
[src/backend/utils/fmgr/funcapi.c:655-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L655-L680)

## Overview
Resolves the actual type of ANYARRAY polymorphic type parameter by first determining ANYELEMENT if needed, then finding the corresponding array type.

## Definition
static void resolve_anyarray_from_others(polymorphic_actuals *actuals)

## Detailed Description
This function resolves the ANYARRAY polymorphic type by leveraging the relationship between arrays and their element types. If ANYELEMENT is not yet resolved, it calls resolve_anyelement_from_others() first to determine the element type. Once the element type is known, it uses get_array_type() to find the corresponding array type for that element type.

The function ensures that every element type has a corresponding array type, reporting an error if no array type can be found for the resolved element type.

## Parameters / Member Variables
- : Pointer to polymorphic_actuals structure containing resolved and unresolved polymorphic type OIDs. The function reads anyelement_type and sets anyarray_type.

## Dependencies
- Functions called/Symbols referenced:
  - [resolve_anyelement_from_others](resolve_anyelement_from_others.md): Resolves ANYELEMENT type if not already done
  - [get_array_type](../g/get_array_type.md): Finds the array type corresponding to an element type
  - OidIsValid: Macro to check if an OID is valid
  - ereport/elog: Error reporting functions
  - [format_type_be](../f/format_type_be.md): Formats type OID as string for error messages

- Called from (representative examples):
  - [resolve_polymorphic_tupdesc](resolve_polymorphic_tupdesc.md): When resolving tuple descriptors with polymorphic types
  - [resolve_polymorphic_argtypes](resolve_polymorphic_argtypes.md): When resolving function argument types

## Notes and Other Information
- This is a static function, only used within funcapi.c
- The function has a dependency on resolve_anyelement_from_others, creating a chain of polymorphic type resolution
- Error occurs if the resolved element type doesn't have a corresponding array type in the system catalogs
- Located in src/backend/utils/fmgr/funcapi.c:655-680

## Simplified Source

```c
static void resolve_anyarray_from_others(polymorphic_actuals *actuals) {
    // Step 1: Ensure we have the element type first
    if (!OidIsValid(actuals->anyelement_type)) {
        resolve_anyelement_from_others(actuals);
    }

    // Step 2: Get the array type for the resolved element type
    if (OidIsValid(actuals->anyelement_type)) {
        Oid array_typeid = get_array_type(actuals->anyelement_type);

        if (!OidIsValid(array_typeid)) {
            // Error: No array type exists for this element type
            ereport(ERROR, "could not find array type for data type");
        }

        actuals->anyarray_type = array_typeid;
    } else {
        // Error: Could not determine element type
        elog(ERROR, "could not determine polymorphic type");
    }
}
```