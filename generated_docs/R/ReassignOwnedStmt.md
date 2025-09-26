# ReassignOwnedStmt

## Location
[src/include/nodes/parsenodes.h:4085-4090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4085-L4090)

## Overview
ReassignOwnedStmt represents the parsed representation of a REASSIGN OWNED statement, which transfers ownership of database objects from specified roles to a new owner role.

## Definition
```c
typedef struct ReassignOwnedStmt
{
    NodeTag     type;
    List       *roles;
    RoleSpec   *newrole;
} ReassignOwnedStmt;
```

## Detailed Description
The ReassignOwnedStmt structure is a parse node that encapsulates the information needed to execute a REASSIGN OWNED BY statement in PostgreSQL. This statement transfers ownership of all database objects owned by one or more specified roles to a new owner role. This is particularly useful for role management scenarios where objects need to be transferred between roles, such as when reorganizing database ownership or preparing to drop a role.

The structure follows PostgreSQL's standard parse node pattern and contains the essential information for the ownership transfer: the list of current owner roles and the specification of the new owner role.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ReassignOwnedStmt parse node
- `roles`: List of RoleSpec nodes representing the current owner roles whose objects should be reassigned
- `newrole`: RoleSpec node specifying the new owner role to which objects will be transferred

## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpec](RoleSpec.md) (structure representing role specifications)
  - NodeTag (for parse node identification) 
  - [List](../L/List.md) (PostgreSQL's generic list structure)

- Called from (representative examples):
  - [ReassignOwnedObjects](ReassignOwnedObjects.md) (executes the REASSIGN OWNED command)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (standard utility command processing)

## Notes and Other Information
- This statement is commonly used in role management scenarios, particularly when reorganizing database ownership or preparing roles for deletion
- The new owner role must have the necessary privileges to own the objects being transferred
- All objects owned by the specified roles (tables, functions, types, etc.) will be transferred to the new owner
- This operation is often used as an alternative to DROP OWNED when you want to preserve the objects but change their ownership
- The statement can handle multiple source roles, allowing bulk reassignment from several roles to a single new owner
- Part of PostgreSQL's role and privilege management system, defined in the parsenodes.h header file