# transformFkeyCheckAttrs

## Location
src/backend/commands/tablecmds.c: 12044 - 12182

## Overview
Validates that the specified columns in a referenced table can support a foreign key constraint by finding a suitable unique index and returning its opclasses.

## Definition


## Detailed Description
This function validates that the specified attribute numbers (columns) in the primary key relation can be properly referenced by a foreign key constraint. It searches through all unique indexes on the referenced table to find one that matches the given columns exactly. The function ensures the foreign key constraint follows SQL standards by rejecting duplicate column references and deferrable unique constraints. Upon finding a suitable index, it populates the caller-provided opclasses array with the operator classes associated with the index columns.

The validation process includes:
- Checking for duplicate column references (forbidden by SQL standard)
- Finding a unique, non-partial, non-expression index that matches the specified columns
- Ensuring the index is not deferrable (per SQL specification)
- Extracting and returning the appropriate operator classes for type compatibility

## Parameters / Member Variables
- : The relation (table) being referenced by the foreign key
- : Number of attributes (columns) in the foreign key
- : Array of attribute numbers representing the referenced columns
- : Output array to be populated with operator classes from the matching index

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - heap_attisnull
  - SysCacheGetAttrNotNull
  - list_free
- Called from (representative examples):
  - ATAddForeignKeyConstraint

## Notes and Other Information
- Returns InvalidOid and raises ERROR if no suitable unique index is found
- Specifically rejects deferrable unique constraints per SQL specification
- The function ensures one-to-one column matching between foreign key and unique index
- Handles indexes with columns in any order relative to the foreign key specification
- Part of the foreign key constraint validation process in table alteration commands