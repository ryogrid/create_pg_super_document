# TupleDescInitEntryCollation

## Location
[src/backend/access/common/tupdesc.c:833-857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L833-L857)

## Overview
Assigns a nondefault collation to a previously initialized tuple descriptor attribute entry.

## Definition
```c
void TupleDescInitEntryCollation(TupleDesc desc,
                                AttrNumber attributeNumber,
                                Oid collationid)
```

## Detailed Description
TupleDescInitEntryCollation is a simple utility function that modifies the collation setting of an already initialized tuple descriptor attribute. This function is typically used as a follow-up step after creating a tuple descriptor entry with TupleDescInitEntry or TupleDescInitBuiltinEntry when a specific (non-default) collation is required for the attribute.

The function performs minimal validation and directly sets the attcollation field of the specified attribute to the provided collation OID. It assumes the tuple descriptor and attribute have already been properly initialized.

## Parameters / Member Variables
- `desc`: The tuple descriptor containing the attribute to modify
- `attributeNumber`: The 1-based position of the attribute within the tuple descriptor
- `collationid`: The OID of the collation to assign to the attribute

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (validation macro)
  - TupleDescAttr (tuple descriptor accessor macro)
- Called from (representative examples):
  - [BuildDescFromLists](../B/BuildDescFromLists.md) (tuple descriptor construction)
  - [initGinState](../i/initGinState.md) (GIN index initialization)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md) (executor type handling)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md) (function relation handling)
  - [resolve_polymorphic_tupdesc](../r/resolve_polymorphic_tupdesc.md) (polymorphic type resolution)

## Notes and Other Information
- This function must be called on an already initialized tuple descriptor entry
- Only modifies the collation field; all other attribute properties remain unchanged
- Commonly used in conjunction with TupleDescInitEntry when building complex tuple descriptors
- Essential for proper handling of collatable data types in PostgreSQL
- No validation is performed on the collation OID itself - it's assumed to be valid

## Simplified Source

```c
void TupleDescInitEntryCollation(TupleDesc desc,
                                AttrNumber attributeNumber,
                                Oid collationid)
{
    // Validate inputs
    Assert(PointerIsValid(desc));
    Assert(attributeNumber >= 1);
    Assert(attributeNumber <= desc->natts);

    // Set the collation for the specified attribute
    TupleDescAttr(desc, attributeNumber - 1)->attcollation = collationid;
}
```