# pg_get_object_address

## Location
[src/backend/catalog/objectaddress.c:2100-2381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2100-L2381)

## Overview
SQL-callable function that converts text-based object identifiers into PostgreSQL's internal ObjectAddress structure, providing a standardized way to identify database objects from SQL commands.

## Definition

```c
struct_array_builtin(namearr, TEXTOID, &elems, &nulls, &nelems);
```
## Detailed Description
The  function serves as the SQL interface to PostgreSQL's internal object identification system. It takes three parameters: an object type string, an array of names, and an array of arguments, then converts these into an ObjectAddress structure that PostgreSQL uses internally to uniquely identify database objects.

This function handles the complex task of parsing different object types and their various naming conventions, validating input parameters, and constructing appropriate node structures for the internal  function. It supports a wide range of PostgreSQL objects including tables, functions, operators, types, and many others, each with their specific parsing requirements.

The function performs extensive validation on input parameters, checking array lengths and null values according to each object type's requirements. It handles special cases for different object types, such as type names for domains and casts, large object OIDs, and complex argument structures for functions and operators.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (text): String representation of the object type (e.g., 'table', 'function', 'operator')
  -  (text[]): Array of name components identifying the object
  -  (text[]): Array of argument type names (for functions, operators, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [read_objtype_from_string](../r/read_objtype_from_string.md)
  - [textarray_to_strvaluelist](../t/textarray_to_strvaluelist.md)
  - [typeStringToTypeName](../t/typeStringToTypeName.md)
  - [get_object_address](../g/get_object_address.md)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [relation_close](../r/relation_close.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- Returns a composite type with three fields: classId (Oid), objectId (Oid), and objectSubId (int32)
- Handles over 30 different object types, each with specific validation and parsing rules
- Special handling for complex objects like functions (with argument lists), operators (with operand types), and casts (with source/target types)
- Performs comprehensive input validation with detailed error messages for invalid parameters
- Uses AccessShareLock when resolving object addresses to ensure consistency
- Part of PostgreSQL's object identification infrastructure, commonly used by DDL commands and system functions

## Simplified Source

```c
Datum
pg_get_object_address(PG_FUNCTION_ARGS)
{
    char       *ttype = TextDatumGetCString(PG_GETARG_DATUM(0));
    ArrayType  *namearr = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType  *argsarr = PG_GETARG_ARRAYTYPE_P(2);
    ObjectType  type;
    List       *name = NIL;
    TypeName   *typename = NULL;
    List       *args = NIL;
    Node       *objnode = NULL;
    ObjectAddress addr;
    HeapTuple   htup;
    Relation    relation;

    // Parse object type from string
    int itype = read_objtype_from_string(ttype);
    if (itype < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unsupported object type \"%s\"", ttype)));
    type = (ObjectType) itype;

    // Handle special object types with specific name parsing
    if (type == OBJECT_TYPE || type == OBJECT_DOMAIN || type == OBJECT_CAST ||
        type == OBJECT_TRANSFORM || type == OBJECT_DOMCONSTRAINT)
    {
        // These types expect a single typename
        Datum *elems;
        bool  *nulls;
        int    nelems;

        deconstruct_array_builtin(namearr, TEXTOID, &elems, &nulls, &nelems);
        if (nelems != 1 || nulls[0])
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid name specification")));
        typename = typeStringToTypeName(TextDatumGetCString(elems[0]), NULL);
    }
    else if (type == OBJECT_LARGEOBJECT)
    {
        // Large objects use OID specification
        Datum *elems;
        bool  *nulls;
        int    nelems;

        deconstruct_array_builtin(namearr, TEXTOID, &elems, &nulls, &nelems);
        if (nelems != 1 || nulls[0])
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("large object OID may not be null")));
        objnode = (Node *) makeFloat(TextDatumGetCString(elems[0]));
    }
    else
    {
        // Most object types use simple string lists
        name = textarray_to_strvaluelist(namearr);
        if (name == NIL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("name list length must be at least 1")));
    }

    // Parse arguments based on object type
    if (type == OBJECT_FUNCTION || type == OBJECT_PROCEDURE ||
        type == OBJECT_AGGREGATE || type == OBJECT_OPERATOR ||
        type == OBJECT_CAST || type == OBJECT_AMOP || type == OBJECT_AMPROC)
    {
        // These types expect TypeName arguments
        Datum *elems;
        bool  *nulls;
        int    nelems;
        int    i;

        deconstruct_array_builtin(argsarr, TEXTOID, &elems, &nulls, &nelems);
        args = NIL;
        for (i = 0; i < nelems; i++)
        {
            if (nulls[i])
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("argument lists may not contain nulls")));
            args = lappend(args, typeStringToTypeName(TextDatumGetCString(elems[i]), NULL));
        }
    }
    else
    {
        // Other types use string arguments
        args = textarray_to_strvaluelist(argsarr);
    }

    // Build appropriate node structure based on object type
    switch (type)
    {
        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
        case OBJECT_VIEW:
        // ... (many relation-like objects)
            objnode = (Node *) name;
            break;

        case OBJECT_DATABASE:
        case OBJECT_ROLE:
        case OBJECT_SCHEMA:
        // ... (simple named objects)
            if (list_length(name) != 1)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("name list length must be exactly 1")));
            objnode = linitial(name);
            break;

        case OBJECT_TYPE:
        case OBJECT_DOMAIN:
            objnode = (Node *) typename;
            break;

        case OBJECT_FUNCTION:
        case OBJECT_PROCEDURE:
        case OBJECT_AGGREGATE:
        case OBJECT_OPERATOR:
            {
                ObjectWithArgs *owa = makeNode(ObjectWithArgs);
                owa->objname = name;
                owa->objargs = args;
                objnode = (Node *) owa;
                break;
            }

        // ... (other object types)
    }

    if (objnode == NULL)
        elog(ERROR, "unrecognized object type: %d", type);

    // Get the actual object address
    addr = get_object_address(type, objnode, &relation, AccessShareLock, false);

    if (relation)
        relation_close(relation, AccessShareLock);

    // Return as composite type (classId, objectId, objectSubId)
    TupleDesc tupdesc;
    Datum     values[3];
    bool      nulls[3];

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    values[0] = ObjectIdGetDatum(addr.classId);
    values[1] = ObjectIdGetDatum(addr.objectId);
    values[2] = Int32GetDatum(addr.objectSubId);
    nulls[0] = nulls[1] = nulls[2] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```