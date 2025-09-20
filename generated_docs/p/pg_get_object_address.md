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