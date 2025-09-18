# getObjectIdentityParts

## Location
src/backend/catalog/objectaddress.c: 4755 - 5964

## Overview
A comprehensive function that generates detailed identity information for database objects, returning both a complete identity string and optionally decomposed object name and argument lists suitable for reconstructing the ObjectAddress.

## Definition
```c
char *getObjectIdentityParts(const ObjectAddress *object, List **objname, List **objargs, bool missing_ok)
```

## Detailed Description
This function is the core implementation for object identity generation in PostgreSQL, handling all major database object types through a comprehensive switch statement based on the object's class ID. It constructs a human-readable identity string while optionally providing decomposed components that can be used with get_object_address() to reconstruct the original ObjectAddress.

The function handles over 30 different object types including relations, procedures, types, casts, collations, constraints, conversions, languages, operators, access methods, namespaces, users, databases, extensions, and many others. For each object type, it performs catalog lookups to retrieve the necessary information and formats it appropriately with schema qualification when needed.

The dual return mechanism allows for both display purposes (the string) and programmatic reconstruction (the lists), making it suitable for various use cases including object identification, event triggers, and system catalog operations.

## Parameters / Member Variables
- `object`: Pointer to an ObjectAddress structure containing the object's class ID, object ID, and sub-object ID
- `objname`: Output parameter for a list of C-strings representing the object name components (can be NULL)
- `objargs`: Output parameter for a list of C-strings representing the object arguments (can be NULL)
- `missing_ok`: Boolean flag indicating whether to handle missing objects gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [get_attname](get_attname.md) (attribute name lookup)
  - [getRelationIdentity](getRelationIdentity.md) (relation identity formatting)
  - [format_procedure_extended](../f/format_procedure_extended.md) (procedure formatting)
  - [format_type_extended](../f/format_type_extended.md) (type formatting)
  - [format_operator_extended](../f/format_operator_extended.md) (operator formatting)
  - [getOpFamilyIdentity](getOpFamilyIdentity.md) (operator family identity)
  - [GetAttrDefaultColumnAddress](../G/GetAttrDefaultColumnAddress.md) (attribute default lookup)
  - [LargeObjectExists](../L/LargeObjectExists.md) (large object existence check)
  - [GetForeignDataWrapperExtended](../G/GetForeignDataWrapperExtended.md) (FDW lookup)
  - [GetForeignServerExtended](../G/GetForeignServerExtended.md) (foreign server lookup)
  - [GetUserNameFromId](../G/GetUserNameFromId.md) (user name lookup)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name lookup)
  - [get_database_name](get_database_name.md) (database name lookup)
  - [get_tablespace_name](get_tablespace_name.md) (tablespace name lookup)
  - [get_extension_name](get_extension_name.md) (extension name lookup)
  - [get_publication_name](get_publication_name.md) (publication name lookup)
  - [get_subscription_name](get_subscription_name.md) (subscription name lookup)
  - [getPublicationSchemaInfo](getPublicationSchemaInfo.md) (publication schema information)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting)
  - quote_qualified_identifier (qualified identifier quoting)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - [get_catalog_object_by_oid](get_catalog_object_by_oid.md) (catalog object retrieval)
  - Various catalog form structures (Form_pg_*)

- Called from (representative examples):
  - [getObjectIdentity](getObjectIdentity.md) (simplified interface wrapper)
  - pg_identify_object_as_address (SQL function for address-based identification)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md) (event trigger system)
  - ObjectAddressSet (object address utility)

## Notes and Other Information
- This is a very large function (over 1200 lines) that serves as the central dispatcher for object identity generation
- The function uses extensive catalog lookups and system cache operations for performance
- Proper error handling with missing_ok parameter allows graceful degradation when objects don't exist
- Schema qualification is automatically applied when necessary for unambiguous identification
- The function supports recursive calls for complex objects like constraints on domains
- Memory management follows PostgreSQL conventions with palloc'd strings that must be freed by caller
- Output lists use PostgreSQL's List structure and associated utility functions like list_make1, list_make2, etc.
- Part of PostgreSQL's comprehensive object address and identification infrastructure
- Critical component for event triggers, dependency tracking, and object management operations