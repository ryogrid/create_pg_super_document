# isObjectPinned

## Location
src/backend/catalog/pg_depend.c: 710 - 732

## Overview
Tests if a database object is required for basic database functionality and is therefore "pinned" against deletion.

## Definition


## Detailed Description
The  function determines whether a given database object is considered "pinned", meaning it is essential for basic database operations and cannot be dropped by users. This function serves as a wrapper around the lower-level  function, adapting the  structure format to the underlying implementation.

The function specifically ignores any subId component of the object address, operating under the assumption that if an object is pinned, all of its components are implicitly pinned as well. This design choice simplifies the pinning logic by treating objects as atomic units for dependency purposes.

## Parameters / Member Variables
- : A pointer to an  structure containing the class ID and object ID of the database object to test for pinned status

## Dependencies
- Functions called/Symbols referenced:
  - IsPinnedObject
- Called from (representative examples):
  - recordMultipleDependencies
  - changeDependencyFor
  - changeDependenciesOn

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_depend.c file
- The function deliberately ignores the subId field of the ObjectAddress, treating whole objects as the unit of pinning
- Pinned objects are typically system catalogs and other core database infrastructure that must remain intact for the database to function properly
- The pinning mechanism is a critical safety feature that prevents accidental deletion of essential system objects