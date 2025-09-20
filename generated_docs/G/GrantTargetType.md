# GrantTargetType

## Location
[src/include/nodes/parsenodes.h:2489-2490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2489-L2490)

## Overview
GrantTargetType is an enumeration that defines the different types of targets that can be specified in PostgreSQL GRANT and REVOKE statements.

## Definition

```c
typedef struct GrantStmt
{
	NodeTag		type;
	bool		is_grant;		/* true = GRANT, false = REVOKE */
	GrantTargetType targtype;	/* type of the grant target */
	ObjectType	objtype;		/* kind of object being operated on */
	List	   *objects;		/* list of RangeVar nodes, ObjectWithArgs
								 * nodes, or plain names (as String values) */
	List	   *privileges;		/* list of AccessPriv nodes */
	/* privileges == NIL denotes ALL PRIVILEGES */
	List	   *grantees;		/* list of RoleSpec nodes */
	bool		grant_option;	/* grant or revoke grant option */
	RoleSpec   *grantor;
	DropBehavior behavior;		/* drop behavior (for REVOKE) */
} GrantStmt;
```
## Detailed Description
This enumeration specifies the scope of GRANT and REVOKE operations in PostgreSQL's access control system. It determines whether the privilege operation applies to specific named objects, all objects within schemas, or default privileges for future objects. The enum is used within the GrantStmt structure to distinguish between different forms of privilege management statements.

The three target types correspond to different SQL syntax patterns: granting privileges on specific objects (e.g., GRANT SELECT ON table1 TO user1), granting privileges on all objects of a type within schemas (e.g., GRANT SELECT ON ALL TABLES IN SCHEMA public TO user1), and setting default privileges for future objects (e.g., ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO user1).

## Parameters / Member Variables
- : Grants or revokes privileges on specifically named database objects. This is used when the GRANT/REVOKE statement explicitly lists the objects to be affected (tables, functions, sequences, etc.).

- : Grants or revokes privileges on all objects of a specified type within one or more schemas. This corresponds to the "GRANT ... ON ALL ... IN SCHEMA" syntax.

- : Used for ALTER DEFAULT PRIVILEGES statements, which set the privileges that will be automatically granted on objects created in the future by specified roles within specified schemas.

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Called from (representative examples):
  -  structure in src/include/nodes/parsenodes.h:2495

## Notes and Other Information
- This enum is defined in src/include/nodes/parsenodes.h:2484-2489
- The enum is used as the  field in the  structure to specify what kind of grant target is being used
- Each target type corresponds to different SQL syntax patterns in GRANT/REVOKE statements
- The ACL prefix in the enum values stands for Access Control List, reflecting PostgreSQL's privilege management system
- This enum is essential for the parser to distinguish between different forms of privilege grant/revoke operations
- The enum values help route the privilege operations to appropriate handling code based on the scope of the operation