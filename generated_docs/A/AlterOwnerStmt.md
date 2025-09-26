# AlterOwnerStmt

## Location
src/include/nodes/parsenodes.h: 3571 - 3578

## Overview
AlterOwnerStmt is a PostgreSQL parse node structure that represents an ALTER object OWNER TO statement for changing the ownership of database objects.

## Definition


## Detailed Description
AlterOwnerStmt represents SQL statements that transfer ownership of database objects from one role to another. This operation is fundamental for database administration and access control, allowing administrators to reassign ownership of tables, functions, schemas, and other database objects. The ownership change affects permissions and privileges associated with the object, as the new owner gains full control over the object.

## Parameters / Member Variables
- : Standard NodeTag for parse tree identification
- : Specifies the type of object whose ownership is being changed (OBJECT_TABLE, OBJECT_TYPE, etc.)
- : RangeVar pointer used when the object being transferred is a table or relation
- : Generic Node pointer for other types of objects being transferred
- : RoleSpec pointer specifying the new owner role

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration for database object types)
  - RangeVar (structure for table/relation references)
  - RoleSpec (structure for role specification)
  - NodeTag (standard parse node identification)
- Called from (representative examples):
  - ExecAlterOwnerStmt (main execution function)
  - standard_ProcessUtility (utility command processing)
  - ProcessUtilitySlow (utility command processing)

## Notes and Other Information
Ownership changes in PostgreSQL have significant security implications, as the new owner gains full privileges over the object. The operation requires appropriate permissions - typically only superusers or the current owner can transfer ownership. The RoleSpec allows for flexible role specification, supporting both explicit role names and special constructs like CURRENT_USER or SESSION_USER. This statement is commonly used in database migration scripts and administrative operations.