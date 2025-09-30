# get_object_attnum_namespace

## Location
[src/backend/catalog/objectaddress.c:2668-2675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2668-L2675)

## Overview
Returns the attribute number for the namespace field of a given PostgreSQL object class, enabling operations to locate the namespace identifier within catalog table structures.

## Definition

```c
AttrNumber
get_object_attnum_namespace(Oid class_id)
```
## Detailed Description
This function serves as an accessor to retrieve the namespace attribute number for a specific PostgreSQL object class. It acts as a wrapper around the object property data system, providing a clean interface to obtain the column number that stores the namespace OID in the corresponding system catalog table. This information is crucial for operations that need to identify or manipulate the namespace (schema) to which an object belongs.

The function leverages the centralized object property system through  to maintain consistency across different object types and their catalog representations.

## Parameters / Member Variables
- : The OID of the object class (typically a system catalog table OID like RelationRelationId, ProcedureRelationId, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md)
  - ObjectPropertyType (struct)
- Called from (representative examples):
  - [pg_identify_object](../p/pg_identify_object.md)
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md)
  - [AlterObjectNamespace_oid](../A/AlterObjectNamespace_oid.md)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md)
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md)
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md)
  - ObjectAddressSet

## Notes and Other Information
- Returns the attribute number as stored in the ObjectPropertyType structure's  field
- Part of the object address subsystem that provides uniform access to object metadata
- Essential for namespace-related operations like schema changes, object identification, and event trigger processing
- The returned attribute number corresponds to the column position in the system catalog table where the namespace OID is stored

## Simplified Source

```c
AttrNumber get_object_attnum_namespace(Oid class_id) {
    // Get object property metadata for the catalog class
    const ObjectPropertyType *prop = get_object_property_data(class_id);

    // Return the attribute number for the namespace column
    return prop->attnum_namespace;
}
```