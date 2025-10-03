# construct_array_builtin

## Location
[src/backend/utils/adt/arrayfuncs.c:3381-3481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3381-L3481)

## Overview
Creates a 1-dimensional array object from Datum elements for built-in PostgreSQL data types, automatically determining type-specific properties without requiring explicit type information.

## Definition

```c
ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
```
## Detailed Description
The construct_array_builtin function provides a specialized version of construct_array that works specifically with PostgreSQL's built-in data types. It eliminates the need for callers to provide explicit elmlen, elmbyval, and elmalign parameters by looking up these values from hardcoded data based on the element type OID. This function is particularly useful when working with system catalog data where the element types are known built-in types.

The function uses a switch statement to map common built-in type OIDs to their corresponding type properties, then delegates the actual array construction to construct_array. If an unsupported type is provided, the function raises an ERROR.

## Parameters / Member Variables
- `*elems`: Array of Datum items that will become the contents of the constructed array (NULL values not supported)
- `nelems`: Number of items in the elems array
- `elmtype`: OID of the built-in data type for the array elements
## Dependencies
- Functions called/Symbols referenced:
  - [construct_array](construct_array.md)
  - TYPALIGN_CHAR (alignment constant)
  - TYPALIGN_INT (alignment constant)
  - TYPALIGN_SHORT (alignment constant)
  - TYPALIGN_DOUBLE (alignment constant)
  - FLOAT8PASSBYVAL (platform-specific constant)
  - NAMEDATALEN (constant for name data length)
- Called from (representative examples):
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - [update_attstats](../u/update_attstats.md)
  - [filter_list_to_array](../f/filter_list_to_array.md)
  - [convert_requires_to_datum](convert_requires_to_datum.md)
  - [pg_extension_config_dump](../p/pg_extension_config_dump.md)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md)
  - [CreateFunction](../C/CreateFunction.md)
  - [CreatePolicy](../C/CreatePolicy.md)
  - [build_regtype_array](../b/build_regtype_array.md)
  - [publicationListToArray](../p/publicationListToArray.md)
  - [current_schemas](current_schemas.md)
  - [GUCArrayAdd](../G/GUCArrayAdd.md)
  - [GUCArrayDelete](../G/GUCArrayDelete.md)

## Notes and Other Information
- Supported built-in types include: CHAR, CSTRING, FLOAT4, INT2, INT4, INT8, NAME, OID, REGTYPE, TEXT, and TID
- The function will raise an ERROR for any unsupported type OID
- This is commonly used when manipulating arrays in system catalog operations where type information is predictable
- Provides better performance than looking up type information from system catalogs for known built-in types
- The hardcoded type information matches the definitions in PostgreSQL's type system

## Simplified Source

```c
ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
    int elmlen;
    bool elmbyval;
    char elmalign;

    // Set type properties based on built-in type OID
    switch (elmtype)
    {
        case CHAROID:
            elmlen = 1;
            elmbyval = true;
            elmalign = TYPALIGN_CHAR;
            break;

        case CSTRINGOID:
            elmlen = -2;  // Variable length, null-terminated
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
            break;

        case FLOAT4OID:
            elmlen = sizeof(float4);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case INT2OID:
            elmlen = sizeof(int16);
            elmbyval = true;
            elmalign = TYPALIGN_SHORT;
            break;

        case INT4OID:
            elmlen = sizeof(int32);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case INT8OID:
            elmlen = sizeof(int64);
            elmbyval = FLOAT8PASSBYVAL;  // Platform dependent
            elmalign = TYPALIGN_DOUBLE;
            break;

        case NAMEOID:
            elmlen = NAMEDATALEN;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
            break;

        case OIDOID:
        case REGTYPEOID:
            elmlen = sizeof(Oid);
            elmbyval = true;
            elmalign = TYPALIGN_INT;
            break;

        case TEXTOID:
            elmlen = -1;  // Variable length
            elmbyval = false;
            elmalign = TYPALIGN_INT;
            break;

        case TIDOID:
            elmlen = sizeof(ItemPointerData);
            elmbyval = false;
            elmalign = TYPALIGN_SHORT;
            break;

        default:
            elog(ERROR, "type %u not supported by construct_array_builtin()", elmtype);
    }

    // Delegate to standard array construction
    return construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);
}
```