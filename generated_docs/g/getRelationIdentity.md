# getRelationIdentity

## Location
src/backend/catalog/objectaddress.c: 6009 - 6042

## Overview
A static helper function that appends a quoted, schema-qualified relation name to a StringInfo buffer, providing a standardized way to identify database relations (tables, views, indexes, etc.) in PostgreSQL.

## Definition
```c
static void getRelationIdentity(StringInfo buffer, Oid relid, List **object, bool missing_ok)
```

## Detailed Description
This function constructs a properly formatted identity string for database relations by looking up the relation in the pg_class system catalog and retrieving both the relation name and its namespace. The output is a schema-qualified identifier that is properly quoted to handle special characters or reserved words. The function serves as a building block for more complex object identification operations throughout PostgreSQL's object address system.

The function handles various types of relations including tables, views, indexes, sequences, materialized views, and other relation-like objects that are stored in pg_class. It ensures consistent formatting and proper quoting to create unambiguous relation identifiers suitable for display, logging, or programmatic reconstruction.

## Parameters / Member Variables
- `buffer`: StringInfo buffer where the quoted qualified relation name will be appended
- `relid`: Object ID (OID) of the relation to identify
- `object`: Optional output parameter for a list containing the schema name and relation name (can be NULL)
- `missing_ok`: Boolean flag indicating whether to handle missing relations gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup for relation)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging)
  - GETSTRUCT (tuple data extraction)
  - ObjectIdGetDatum (OID conversion)
  - get_namespace_name_or_temp (namespace name lookup)
  - appendStringInfoString (string buffer operations)
  - quote_qualified_identifier (schema-qualified identifier quoting)
  - NameStr (name extraction from catalog form)
  - list_make2 (two-element list creation)
  - pstrdup (string duplication)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_class (relation catalog structure)
  - NIL (empty list constant)

- Called from (representative examples):
  - getObjectIdentityParts (relation case and attribute handling)
  - getObjectIdentityParts (constraint on table handling)
  - getObjectIdentityParts (rule on table handling)
  - getObjectIdentityParts (trigger on table handling)
  - getObjectIdentityParts (policy on table handling)
  - getObjectIdentityParts (publication relation handling)
  - object_type_map (object type mapping structure)

## Notes and Other Information
- This is a static function, accessible only within the objectaddress.c compilation unit
- The function uses PostgreSQL's system cache for efficient pg_class lookups
- Output format is always schema-qualified to ensure unambiguous identification
- When missing_ok is true and the relation is not found, the object list is set to NIL and the function returns without appending to the buffer
- The function handles all types of relations stored in pg_class (tables, views, indexes, sequences, etc.)
- Proper cache management is implemented with ReleaseSysCache to prevent memory leaks
- When the object parameter is provided, it returns a two-element list containing schema name and relation name
- Part of PostgreSQL's comprehensive relation identification infrastructure used throughout the system
- Widely used by getObjectIdentityParts for various object types that reference relations