# get_object_attnum_name

## Location
src/backend/catalog/objectaddress.c: 2660 - 2667

## Overview
Retrieves the attribute number (column number) that stores the object name in the catalog table for a given object class.

## Definition
```c
AttrNumber get_object_attnum_name(Oid class_id)
```

## Detailed Description
This function returns the attribute number (column position) within a catalog table that contains the object's name. Just as `get_object_attnum_oid` provides the column number for the OID, this function identifies which column stores the object's name in the corresponding catalog table. This is essential for operations that need to access or modify object names in a generic way across different catalog tables.

The function accesses the object property metadata and returns the `attnum_name` field, which specifies the column number where the object's name is stored in the catalog table.

## Parameters / Member Variables
- `class_id`: The OID of the catalog class (typically a system catalog table OID) for which to retrieve the name attribute number

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md): Retrieves object property metadata
  - `ObjectPropertyType`: Structure containing object property information
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md): Used in privilege operations to access object names
  - `pg_identify_object`: Used in object identification to retrieve names
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md): Used during object renaming to locate the name column
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md): Used during namespace changes to access names
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md): Used during ownership changes to identify objects
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md): Used in event trigger processing to get object names

## Notes and Other Information
- Returns an `AttrNumber` indicating the column position of the name field
- Complements `get_object_attnum_oid` by providing access to the name column rather than OID column
- Essential for DDL operations that involve object renaming, namespace changes, or name-based identification
- Used extensively in system catalog manipulation functions that need to work generically across object types
- The name column typically contains the primary identifier by which users reference the object
- Column numbering follows PostgreSQL convention starting from 1
- Critical for maintaining consistency in name-based operations across different catalog table structures