# typeTypeId

## Location
[src/backend/parser/parse_type.c:590-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L590-L598)

## Overview
typeTypeId extracts the OID from a Type structure (pg_type system catalog tuple), providing the reverse operation of typeidType.

## Definition
```c
Oid typeTypeId(Type tp)
```

## Detailed Description
This function is a simple accessor that extracts the OID field from a Type structure. It takes a Type (which is actually a HeapTuple containing a pg_type catalog row) and returns the type's OID by accessing the oid field of the underlying Form_pg_type structure.

The function includes a NULL check to prevent crashes, though as noted in the code comment, this check is "probably useless" since callers should generally ensure they have a valid Type structure.

## Parameters / Member Variables
- `tp`: Type structure (HeapTuple) containing pg_type catalog data; must not be NULL

## Dependencies
- Functions called/Symbols referenced:
  - Type (parameter type)
  - Form_pg_type (for accessing tuple structure)
  - GETSTRUCT (macro to extract structure from tuple)
  - elog (for error reporting)
- Called from (representative examples):
  - [get_object_address_type](../g/get_object_address_type.md)
  - [compute_return_type](../c/compute_return_type.md)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md)
  - [AlterTypeOwner](../A/AlterTypeOwner.md)
  - [AlterType](../A/AlterType.md)
  - [FuncNameAsType](../F/FuncNameAsType.md)

## Notes and Other Information
- Performs the inverse operation of typeidType: converts Type structure back to OID
- Includes defensive NULL checking, though this situation should not normally occur
- Uses GETSTRUCT macro to safely extract the Form_pg_type structure from the HeapTuple
- Simple accessor function primarily used in type management and DDL operations
- Located in src/backend/parser/parse_type.c:590-598
- The returned OID can be used for further type system operations or comparisons

## Simplified Source

```c
Oid typeTypeId(Type tp) {
    // Defensive check for NULL input
    if (tp == NULL)
        elog(ERROR, "typeTypeId() called with NULL type struct");

    // Extract OID from the pg_type tuple structure
    return ((Form_pg_type) GETSTRUCT(tp))->oid;
}
```