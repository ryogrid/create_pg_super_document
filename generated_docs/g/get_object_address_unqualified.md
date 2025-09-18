# get_object_address_unqualified

## Location
src/backend/catalog/objectaddress.c: 1242 - 1332

## Overview
Finds an ObjectAddress for database objects that are identified by a simple unqualified name, handling various system-level PostgreSQL objects.

## Definition


## Detailed Description
The  function is a static helper function that handles object address resolution for PostgreSQL objects that are identified by simple, unqualified names. These are typically system-level objects that exist in a global namespace rather than being schema-qualified. The function serves as a centralized dispatcher that maps object types to their corresponding catalog lookup functions.

The function performs a straightforward switch on the object type and calls the appropriate  function for each supported object type. Each case follows the same pattern: setting the appropriate catalog relation ID (classId), calling the type-specific lookup function, and setting objectSubId to 0 since these objects don't have sub-objects.

The supported object types include access methods, databases, extensions, tablespaces, roles, schemas, languages, foreign data wrappers, foreign servers, event triggers, parameter ACLs, publications, and subscriptions. These represent the major categories of PostgreSQL objects that exist at the cluster or database level rather than within specific schemas.

## Parameters / Member Variables
- : The type of object being looked up (from ObjectType enumeration)
- : String node containing the object name
- : If true, return invalid ObjectAddress instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - get_am_oid (for access methods)
  - get_database_oid (for databases)
  - get_extension_oid (for extensions)
  - get_tablespace_oid (for tablespaces)
  - get_role_oid (for roles)
  - get_namespace_oid (for schemas)
  - get_language_oid (for languages)
  - get_foreign_data_wrapper_oid (for foreign data wrappers)
  - get_foreign_server_oid (for foreign servers)
  - get_event_trigger_oid (for event triggers)
  - ParameterAclLookup (for parameter ACLs)
  - get_publication_oid (for publications)
  - get_subscription_oid (for subscriptions)
- Called from (representative examples):
  - get_object_address

## Notes and Other Information
This function is marked static, indicating it's only used within the objectaddress.c module as a helper function. It handles objects that don't require schema qualification because they exist in global namespaces. The function follows a consistent pattern for all object types, making it easy to add new unqualified object types in the future. Each lookup function called by this function is responsible for its own error handling when missing_ok is false.