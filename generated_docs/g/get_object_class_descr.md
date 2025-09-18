# get_object_class_descr

## Location
src/backend/catalog/objectaddress.c: 2620 - 2627

## Overview
Retrieves a human-readable string description for a PostgreSQL system catalog class, providing descriptive names for catalog tables used in error messages and logging.

## Definition


## Detailed Description
The `get_object_class_descr` function serves as a simple interface to retrieve descriptive string names for PostgreSQL's system catalog classes. Given a class OID (which corresponds to a system catalog table like pg_class, pg_proc, pg_type, etc.), it returns a human-readable string description of that catalog.

This function is part of a set of accessor functions that provide interfaces to the ObjectPropertyType structure fields. It leverages the get_object_property_data function to locate the appropriate property data for the given class ID and then returns the class_descr field, which contains the descriptive name.

The function is commonly used in error reporting, logging, and user-facing messages where a technical catalog OID needs to be presented in a more understandable format. For example, instead of showing "relation 1259", it would return "relation" for the pg_class catalog.

## Parameters / Member Variables
- `class_id`: OID of the system catalog class for which to retrieve the description

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md)
  - ObjectPropertyType (structure access)
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md) (src/backend/catalog/aclchk.c:2194)
  - [object_aclmask_ext](../o/object_aclmask_ext.md) (src/backend/catalog/aclchk.c:3163)
  - [object_ownercheck](../o/object_ownercheck.md) (src/backend/catalog/aclchk.c:4170, 4201)
  - [recordExtObjInitPriv](../r/recordExtObjInitPriv.md) (src/backend/catalog/aclchk.c:4553)
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md) (src/backend/catalog/aclchk.c:4992)
  - [DropObjectById](../D/DropObjectById.md) (src/backend/catalog/dependency.c:1207, 1230)

## Notes and Other Information
- Returns a constant string pointer that should not be modified or freed
- Part of the ObjectPropertyType interface family alongside other get_object_* functions
- Used extensively in ACL (Access Control List) operations and dependency management
- The returned string is typically a simple noun describing the object class (e.g., "relation", "function", "type")
- Essential for generating user-friendly error messages and log entries that reference catalog objects
- The function assumes the class_id corresponds to a valid, known catalog class