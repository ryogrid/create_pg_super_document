# IsBinaryCoercibleWithCast

## Location
[src/backend/parser/parse_coerce.c:3047-3154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3047-L3154)

## Overview
Extended variant of IsBinaryCoercible that also returns the OID of the pg_cast entry when determining if a source type can be binary-coercible to a target type.

## Definition

```c
enum type as coercible to ANYENUM */
	if (targettype == ANYENUMOID)
		if (type_is_enum(srctype))
			return true;
```
## Detailed Description
IsBinaryCoercibleWithCast is the core implementation function that determines binary coercibility between PostgreSQL data types. It performs comprehensive checks including hardwired rules for built-in types and polymorphic type handling, followed by lookup in the pg_cast system catalog.

The function implements multiple fast-path optimizations for common scenarios:
- Identity coercion (same types)
- Domain to base type reduction
- Polymorphic type compatibility (ANY*, ANYELEMENT, ANYCOMPATIBLE variants)
- Array type coercion to ANYARRAY variants
- Non-array type coercion to ANYNONARRAY variants
- Enum type coercion to ANYENUM
- Range/multirange type coercion to corresponding ANY variants
- Composite type coercion to RECORD

For types not covered by hardwired rules, it searches pg_cast for an implicit, binary-method cast entry.

## Parameters / Member Variables
- : The OID of the source data type to convert from
- : The OID of the target data type to convert to  
- : Pointer to store the OID of the pg_cast entry if found (set to InvalidOid if hardwired rule or no cast exists)

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md)
  - type_is_array
  - [type_is_enum](../t/type_is_enum.md)
  - [type_is_range](../t/type_is_range.md)
  - [type_is_multirange](../t/type_is_multirange.md)
  - ISCOMPLEX
  - [is_complex_array](../i/is_complex_array.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - Form_pg_cast
  - COERCION_METHOD_BINARY
  - COERCION_CODE_IMPLICIT
- Called from (representative examples):
  - [IsBinaryCoercible](IsBinaryCoercible.md) (src/backend/parser/parse_coerce.c:3036)
  - [CreateCast](../C/CreateCast.md) (src/backend/commands/functioncmds.c:1607, 1623)

## Notes and Other Information
- This is the workhorse function that implements the actual binary coercion logic
- Domain types are automatically reduced to their base types for comparison
- The function handles all polymorphic pseudo-types with specific hardwired rules
- Only implicit casts with binary method qualify as binary coercible
- The castoid output parameter allows callers to know which pg_cast entry was used
- Fast-path optimizations avoid expensive catalog lookups for common type relationships
- Composite arrays are handled specially for RECORD[] coercion compatibility

## Simplified Source

```c
bool
IsBinaryCoercibleWithCast(Oid srctype, Oid targettype,
                          Oid *castoid)
{
    HeapTuple   tuple;
    Form_pg_cast castForm;
    bool        result;

    *castoid = InvalidOid;

    // Fast path: same type
    if (srctype == targettype)
        return true;

    // Handle polymorphic ANY* types
    if (targettype == ANYOID || targettype == ANYELEMENTOID ||
        targettype == ANYCOMPATIBLEOID)
        return true;

    // Reduce domain to base type
    if (OidIsValid(srctype))
        srctype = getBaseType(srctype);

    // Check again after domain reduction
    if (srctype == targettype)
        return true;

    // Handle specific polymorphic type families
    if ((targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID) &&
        type_is_array(srctype))
        return true;

    if ((targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID) &&
        !type_is_array(srctype))
        return true;

    if (targettype == ANYENUMOID && type_is_enum(srctype))
        return true;

    if ((targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID) &&
        type_is_range(srctype))
        return true;

    if ((targettype == ANYMULTIRANGEOID || targettype == ANYCOMPATIBLEMULTIRANGEOID) &&
        type_is_multirange(srctype))
        return true;

    if (targettype == RECORDOID && ISCOMPLEX(srctype))
        return true;

    if (targettype == RECORDARRAYOID && is_complex_array(srctype))
        return true;

    // Look up in pg_cast catalog
    tuple = SearchSysCache2(CASTSOURCETARGET,
                            ObjectIdGetDatum(srctype),
                            ObjectIdGetDatum(targettype));
    if (!HeapTupleIsValid(tuple))
        return false;

    castForm = (Form_pg_cast) GETSTRUCT(tuple);

    // Check if it's an implicit binary cast
    result = (castForm->castmethod == COERCION_METHOD_BINARY &&
              castForm->castcontext == COERCION_CODE_IMPLICIT);

    if (result)
        *castoid = castForm->oid;

    ReleaseSysCache(tuple);
    return result;
}
```