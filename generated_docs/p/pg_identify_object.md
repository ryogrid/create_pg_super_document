# pg_identify_object

## Location
[src/backend/catalog/objectaddress.c:4233-4349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4233-L4349)

## Overview
SQL-level callable function that obtains object type and identity information for a given database object specified by its catalog class ID, object ID, and sub-object ID.

## Definition

```c
Datum
pg_identify_object(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a SQL interface for identifying PostgreSQL database objects. It takes three parameters (classid, objid, objsubid) representing a database object and returns a composite type containing four fields: object type, schema name, object name, and object identity string.

The function first constructs an ObjectAddress from the input parameters, then uses the catalog system to retrieve object metadata. For supported object classes, it opens the appropriate catalog table and extracts the object's namespace and name information. The function only returns the object name if it can be used as a unique identifier along with the schema name.

The return value is a tuple with four elements:
1. Object type description (never NULL)
2. Schema name (NULL if not applicable or not found)
3. Object name (NULL if not applicable, not unique, or not found)
4. Object identity string (NULL if object could not be identified)

## Parameters / Member Variables
-  (Oid): The catalog relation OID that contains the object
-  (Oid): The object's OID within the catalog
-  (int32): Sub-object identifier (typically column number for table columns, 0 for whole objects)

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md)
  - [is_objectclass_supported](../i/is_objectclass_supported.md)
  - [get_catalog_object_by_oid](../g/get_catalog_object_by_oid.md)
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md)
  - [get_object_attnum_namespace](../g/get_object_attnum_namespace.md)
  - [get_object_namensp_unique](../g/get_object_namensp_unique.md)
  - [get_object_attnum_name](../g/get_object_attnum_name.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [getObjectTypeDescription](../g/getObjectTypeDescription.md)
  - [getObjectIdentity](../g/getObjectIdentity.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function exposed to users for object introspection
- The function handles cases where object identity cannot be determined by setting appropriate fields to NULL
- Object names are only returned when they can serve as unique identifiers
- The function uses the catalog system's metadata to determine object properties
- Located in src/backend/catalog/objectaddress.c:4233-4349

## Simplified Source

```c
Datum
pg_identify_object(PG_FUNCTION_ARGS)
{
    Oid         classid = PG_GETARG_OID(0);
    Oid         objid = PG_GETARG_OID(1);
    int32       objsubid = PG_GETARG_INT32(2);
    Oid         schema_oid = InvalidOid;
    const char *objname = NULL;
    char       *objidentity;
    ObjectAddress address;
    Datum       values[4];
    bool        nulls[4];
    TupleDesc   tupdesc;
    HeapTuple   htup;

    // Build object address
    address.classId = classid;
    address.objectId = objid;
    address.objectSubId = objsubid;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Extract schema and name info for supported object classes
    if (is_objectclass_supported(address.classId))
    {
        HeapTuple  objtup;
        Relation   catalog = table_open(address.classId, AccessShareLock);

        objtup = get_catalog_object_by_oid(catalog,
                                          get_object_attnum_oid(address.classId),
                                          address.objectId);
        if (objtup != NULL)
        {
            bool       isnull;
            AttrNumber nspAttnum, nameAttnum;

            // Get schema OID if object has namespace
            nspAttnum = get_object_attnum_namespace(address.classId);
            if (nspAttnum != InvalidAttrNumber)
            {
                schema_oid = heap_getattr(objtup, nspAttnum,
                                         RelationGetDescr(catalog), &isnull);
                if (isnull)
                    elog(ERROR, "invalid null namespace in object %u/%u/%d",
                         address.classId, address.objectId, address.objectSubId);
            }

            // Get object name if it provides unique identification
            if (get_object_namensp_unique(address.classId))
            {
                nameAttnum = get_object_attnum_name(address.classId);
                if (nameAttnum != InvalidAttrNumber)
                {
                    Datum nameDatum = heap_getattr(objtup, nameAttnum,
                                                  RelationGetDescr(catalog), &isnull);
                    if (isnull)
                        elog(ERROR, "invalid null name in object %u/%u/%d",
                             address.classId, address.objectId, address.objectSubId);
                    objname = quote_identifier(NameStr(*(DatumGetName(nameDatum))));
                }
            }
        }
        table_close(catalog, AccessShareLock);
    }

    // Object type (never NULL)
    values[0] = CStringGetTextDatum(getObjectTypeDescription(&address, true));
    nulls[0] = false;

    // Get object identity string
    objidentity = getObjectIdentity(&address, true);

    // Schema name
    if (OidIsValid(schema_oid) && objidentity)
    {
        const char *schema = quote_identifier(get_namespace_name(schema_oid));
        values[1] = CStringGetTextDatum(schema);
        nulls[1] = false;
    }
    else
        nulls[1] = true;

    // Object name
    if (objname && objidentity)
    {
        values[2] = CStringGetTextDatum(objname);
        nulls[2] = false;
    }
    else
        nulls[2] = true;

    // Object identity
    if (objidentity)
    {
        values[3] = CStringGetTextDatum(objidentity);
        nulls[3] = false;
    }
    else
        nulls[3] = true;

    htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```