# resolve_anymultirange_from_others

## Location
[src/backend/utils/fmgr/funcapi.c:710-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L710-L743)

## Overview
Resolves the actual type of ANYMULTIRANGE polymorphic type parameter from other polymorphic inputs, specifically from ANYRANGE type when available.

## Definition
static void resolve_anymultirange_from_others(polymorphic_actuals *actuals)

## Detailed Description
This function resolves the ANYMULTIRANGE polymorphic type with a key limitation similar to its sibling function resolve_anyrange_from_others: it cannot deduce a multirange type from polymorphic array or base element types because multiple range types can share the same subtype. However, it can uniquely determine the multirange type from a range type since each range type has exactly one corresponding multirange type.

When anyrange_type is valid, the function uses get_range_multirange() to find the multirange type that corresponds to the given range type. This provides a direct one-to-one mapping from range to multirange type.

## Parameters / Member Variables
- : Pointer to polymorphic_actuals structure containing resolved and unresolved polymorphic type OIDs. The function reads anyrange_type and sets anymultirange_type.

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md): Gets the base type of a potentially domain type
  - [get_range_multirange](../g/get_range_multirange.md): Finds the multirange type corresponding to a range type
  - OidIsValid: Macro to check if an OID is valid
  - ereport/elog: Error reporting functions
  - [format_type_be](../f/format_type_be.md): Formats type OID as string for error messages

- Called from (representative examples):
  - [resolve_polymorphic_tupdesc](resolve_polymorphic_tupdesc.md): When resolving tuple descriptors with polymorphic types
  - [resolve_polymorphic_argtypes](resolve_polymorphic_argtypes.md): When resolving function argument types

## Notes and Other Information
- This is a static function, only used within funcapi.c
- Cannot deduce multirange type from element or array types due to potential ambiguity in range type selection
- Only works when anyrange_type is already resolved
- Complements resolve_anyrange_from_others which works in the reverse direction (multirange → range)
- Located in src/backend/utils/fmgr/funcapi.c:710-743

## Simplified Source

```c
static void resolve_anymultirange_from_others(polymorphic_actuals *actuals) {
    // Can only resolve from anyrange type (not from element/array types
    // due to potential ambiguity - multiple ranges can have same subtype)
    if (OidIsValid(actuals->anyrange_type)) {
        Oid range_base_type = getBaseType(actuals->anyrange_type);
        Oid multirange_typeid = get_range_multirange(range_base_type);

        if (!OidIsValid(multirange_typeid)) {
            ereport(ERROR, "could not find multirange type for range type");
        }

        actuals->anymultirange_type = multirange_typeid;
    } else {
        elog(ERROR, "could not determine polymorphic type");
    }
}
```