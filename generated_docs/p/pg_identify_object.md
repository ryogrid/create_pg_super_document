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
  - get_call_result_type
  - is_objectclass_supported
  - get_catalog_object_by_oid
  - get_object_attnum_oid
  - get_object_attnum_namespace
  - get_object_namensp_unique
  - get_object_attnum_name
  - heap_getattr
  - getObjectTypeDescription
  - getObjectIdentity
  - quote_identifier
  - get_namespace_name
  - heap_form_tuple
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function exposed to users for object introspection
- The function handles cases where object identity cannot be determined by setting appropriate fields to NULL
- Object names are only returned when they can serve as unique identifiers
- The function uses the catalog system's metadata to determine object properties
- Located in src/backend/catalog/objectaddress.c:4233-4349