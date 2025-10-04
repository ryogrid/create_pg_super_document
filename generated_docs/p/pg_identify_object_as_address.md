# pg_identify_object_as_address

## Location
[src/backend/catalog/objectaddress.c:4350-4412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4350-L4412)

## Overview
SQL-level callable function that obtains object type and structured identity information (names and arguments arrays) for a given database object specified by its catalog class ID, object ID, and sub-object ID.

## Definition
```c
Datum pg_identify_object_as_address(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL interface for identifying PostgreSQL database objects in a structured format. Unlike pg_identify_object which returns a single identity string, this function returns the object's identity as separate arrays of names and arguments that can be used to reconstruct the object address.

The function takes three parameters (classid, objid, objsubid) representing a database object and returns a composite type containing three fields: object type, object names array, and object arguments array.

The return value is a tuple with three elements:
1. Object type description (never NULL)
2. Object names array (empty array if object cannot be identified)
3. Object arguments array (empty array if object cannot be identified or has no arguments)

This structured format is particularly useful for programmatic manipulation of object addresses and for reconstructing complete object references.

## Parameters / Member Variables
- `classid` (Oid): The catalog relation OID that contains the object
- `objid` (Oid): The object's OID within the catalog
- `objsubid` (int32): Sub-object identifier (typically column number for table columns, 0 for whole objects)

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md)
  - [getObjectTypeDescription](../g/getObjectTypeDescription.md)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md)
  - [strlist_to_textarray](../s/strlist_to_textarray.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function exposed to users for structured object introspection
- Provides a more structured alternative to pg_identify_object for programmatic use
- Returns empty arrays instead of NULL when object identity parts cannot be determined
- The names and args arrays can be used to reconstruct object addresses programmatically
- Located in src/backend/catalog/objectaddress.c:4350-4412

## Simplified Source

```c
Datum
pg_identify_object_as_address(PG_FUNCTION_ARGS)
{
    Oid         classid = PG_GETARG_OID(0);
    Oid         objid = PG_GETARG_OID(1);
    int32       objsubid = PG_GETARG_INT32(2);
    ObjectAddress address;
    char       *identity;
    List       *names;
    List       *args;
    Datum       values[3];
    bool        nulls[3];
    TupleDesc   tupdesc;
    HeapTuple   htup;

    // Build object address
    address.classId = classid;
    address.objectId = objid;
    address.objectSubId = objsubid;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Object type (never NULL)
    values[0] = CStringGetTextDatum(getObjectTypeDescription(&address, true));
    nulls[0] = false;

    // Get object identity parts
    identity = getObjectIdentityParts(&address, &names, &args, true);
    if (identity == NULL)
    {
        // Object not found - return empty arrays
        nulls[1] = true;
        nulls[2] = true;
    }
    else
    {
        pfree(identity);  // We only need the parts, not the string

        // Object names array
        if (names != NIL)
            values[1] = PointerGetDatum(strlist_to_textarray(names));
        else
            values[1] = PointerGetDatum(construct_empty_array(TEXTOID));
        nulls[1] = false;

        // Object arguments array
        if (args)
            values[2] = PointerGetDatum(strlist_to_textarray(args));
        else
            values[2] = PointerGetDatum(construct_empty_array(TEXTOID));
        nulls[2] = false;
    }

    htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```