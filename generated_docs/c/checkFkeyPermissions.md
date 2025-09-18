# checkFkeyPermissions

## Location
src/backend/commands/tablecmds.c: 12212 - 12240

## Overview
Validates that the current user has sufficient REFERENCES privileges on the referenced table and columns for creating a foreign key constraint.

## Definition


## Detailed Description
This function performs permission checks to ensure the current user has the necessary REFERENCES privileges to create a foreign key constraint that references specific columns in a target table. It implements a two-tier permission model: first checking for table-level REFERENCES permission (which grants access to all columns), and if that fails, checking for column-level REFERENCES permission on each individually specified column. The function assumes that ownership of the referencing table has already been verified earlier in the process.

The permission verification follows PostgreSQL's standard access control model where REFERENCES privilege can be granted either at the table level (covering all columns) or at individual column levels for more granular control.

## Parameters / Member Variables
- : The referenced relation (table) that the foreign key will point to
- : Array of attribute numbers representing the specific columns being referenced
- : Number of attributes (columns) in the foreign key reference

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - ACL_REFERENCES
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)

## Notes and Other Information
- Assumes the user already owns the referencing table (checked elsewhere)
- Uses efficient short-circuit evaluation: table-level permission check first, then column-level if needed
- Raises appropriate access control errors if insufficient privileges are found
- Part of the security validation process during foreign key constraint creation
- Follows PostgreSQL's hierarchical permission model for database objects