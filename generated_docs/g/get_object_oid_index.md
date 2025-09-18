# get_object_oid_index

## Location
src/backend/catalog/objectaddress.c: 2628 - 2635

## Overview
Retrieves the OID of the unique index used for object identification for a given object class.

## Definition


## Detailed Description
This function returns the OID of the unique index that is used to identify objects of a specific catalog class. Each catalog table has a primary unique index that allows efficient lookup of objects by their OID. This function provides access to that index's OID by consulting the object property metadata for the given class.

The function is a simple accessor that delegates to  to retrieve the object property structure and returns the  field from that structure.

## Parameters / Member Variables
- : The OID of the catalog class (typically a system catalog table OID) for which to retrieve the unique index OID

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves object property metadata
  - : Structure containing object property information
- Called from (representative examples):
  - : Used in ownership checking operations
  - : Used during object deletion
  - : Used for object lookup operations
  - : Used in object address construction

## Notes and Other Information
- This function is part of the PostgreSQL object addressing system that provides uniform access to catalog objects
- The returned index OID corresponds to a unique index on the catalog table that allows efficient object lookup
- This is a lightweight accessor function with minimal overhead
- The function assumes the class_id corresponds to a valid catalog class with associated object properties