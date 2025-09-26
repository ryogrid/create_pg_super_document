# is_objectclass_supported

## Location
[src/backend/catalog/objectaddress.c:2729-2745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2729-L2745)

## Overview
Checks whether the ObjectProperty table contains useful data for a given object class identifier.

## Definition

```c
bool
is_objectclass_supported(Oid class_id)
```
## Detailed Description
This function determines if PostgreSQL has metadata support for a specific object class by searching through the ObjectProperty table. The ObjectProperty table contains information about various database object types such as tables, indexes, functions, etc. The function performs a linear search through the ObjectProperty array to find a matching class_oid.

This is primarily used by the object address subsystem to validate whether operations like object identification, property retrieval, and event trigger processing can be performed for objects of a given class.

## Parameters / Member Variables
- `class_id`: Object identifier (Oid) representing the class of database object to check for support

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length)
  - ObjectPropertyType (via ObjectProperty array access)
- Called from (representative examples):
  - [pg_identify_object](../p/pg_identify_object.md)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md)  
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md)
  - ObjectAddressSet

## Notes and Other Information
- Returns true if the class_id is found in the ObjectProperty table, false otherwise
- The function performs O(n) linear search through the ObjectProperty array
- Located in src/backend/catalog/objectaddress.c:2729-2745
- Essential for object address validation and event trigger functionality