# getObjectTypeDescription

## Location
src/backend/catalog/objectaddress.c: 4413 - 4602

## Overview
Returns a human-readable string that describes the type of PostgreSQL database object specified by an ObjectAddress, supporting all major object classes in the system catalog.

## Definition
```c
char *getObjectTypeDescription(const ObjectAddress *object, bool missing_ok)
```

## Detailed Description
This function provides human-readable type descriptions for PostgreSQL database objects. It takes an ObjectAddress structure and returns a palloc'ed string containing the object type description. The function uses a large switch statement to map catalog relation OIDs to descriptive strings.

For certain complex object types (relations, procedures, constraints), it delegates to specialized helper functions that provide more detailed type information. For simpler object types, it returns static string descriptions.

The function supports all major PostgreSQL object classes including relations, functions, types, operators, access methods, text search objects, security objects, and replication objects. It ensures that a valid description is always returned for supported object classes.

## Parameters / Member Variables
- `object` (const ObjectAddress *): Pointer to ObjectAddress structure containing classId, objectId, and objectSubId
- `missing_ok` (bool): Whether to tolerate missing objects (passed to helper functions)

## Dependencies
- Functions called/Symbols referenced:
  - getRelationTypeDescription
  - getProcedureTypeDescription  
  - getConstraintTypeDescription
- Called from (representative examples):
  - pg_identify_object
  - pg_identify_object_as_address
  - EventTriggerSQLDropAddObject
  - pg_event_trigger_ddl_commands
  - ObjectAddressSet

## Notes and Other Information
- Returns a palloc'ed string that must be freed by the caller
- The ObjectTypeMap should be kept in sync with this function's switch statement
- Throws an error for unsupported object classes
- For complex object types, delegates to specialized functions for detailed type information
- Located in src/backend/catalog/objectaddress.c:4413-4602
- Supports all major PostgreSQL object types including tables, indexes, functions, types, operators, etc.