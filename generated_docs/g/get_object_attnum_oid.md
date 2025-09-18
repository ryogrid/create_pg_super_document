# get_object_attnum_oid

## Location
src/backend/catalog/objectaddress.c: 2652 - 2659

## Overview
Retrieves the attribute number (column number) that stores the object OID in the catalog table for a given object class.

## Definition
```c
AttrNumber get_object_attnum_oid(Oid class_id)
```

## Detailed Description
This function returns the attribute number (column position) within a catalog table that contains the object's OID. In PostgreSQL catalog tables, the OID column may not always be the first column or may have a specific position depending on the table structure. This function provides a way to determine which column contains the object's identifier.

The function consults the object property metadata and returns the `attnum_oid` field, which specifies the column number where the object's OID is stored in the corresponding catalog table.

## Parameters / Member Variables
- `class_id`: The OID of the catalog class (typically a system catalog table OID) for which to retrieve the OID attribute number

## Dependencies
- Functions called/Symbols referenced:
  - `get_object_property_data`: Retrieves object property metadata
  - `ObjectPropertyType`: Structure containing object property information
- Called from (representative examples):
  - `object_ownercheck`: Used in ownership verification to locate the OID column
  - `DropObjectById`: Used during object deletion to identify the OID column
  - `pg_identify_object`: Used in object identification routines
  - `AlterObjectOwner_internal`: Used during ownership changes
  - `EventTriggerSQLDropAddObject`: Used in event trigger processing
  - `pg_event_trigger_ddl_commands`: Used in DDL command event triggers

## Notes and Other Information
- Returns an `AttrNumber` (typically a small positive integer) indicating the column position
- Essential for generic catalog operations that need to identify which column contains the object OID
- Part of PostgreSQL's object addressing system that provides uniform access across different catalog tables
- The attribute number follows PostgreSQL's convention where columns are numbered starting from 1
- Used extensively in system functions that perform generic operations on catalog objects
- Critical for maintaining consistency in object identification across different catalog table layouts