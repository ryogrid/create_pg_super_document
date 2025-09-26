# GrantStmt

## Location
[src/include/nodes/parsenodes.h:2491-2505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2491-L2505)

## Overview
GrantStmt represents the parsed form of GRANT and REVOKE statements, which control access privileges to database objects in PostgreSQL's access control system.

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
GrantStmt is a crucial parse tree node structure that represents both GRANT and REVOKE SQL statements in PostgreSQL's comprehensive access control system. This single structure handles the dual nature of privilege management by using the is_grant boolean flag to distinguish between granting and revoking operations.

The structure is designed to handle PostgreSQL's rich privilege system, which supports fine-grained permissions on various types of database objects including tables, sequences, functions, schemas, tablespaces, and more. The flexible design allows for specifying multiple objects, multiple privileges, and multiple grantees in a single statement.

The structure supports PostgreSQL's advanced privilege features including grant options (allowing grantees to further grant privileges to others), custom grantors (for privilege delegation), and different revocation behaviors (CASCADE vs RESTRICT) that control how dependent privileges are handled when privileges are revoked.

## Parameters / Member Variables
- : NodeTag for node type identification in PostgreSQL's node system
- : Boolean flag distinguishing between GRANT (true) and REVOKE (false) operations
- : GrantTargetType enum indicating the category of target objects (tables, columns, sequences, etc.)
- : ObjectType enum specifying the precise type of database object being operated on
- : List of target objects represented as RangeVar nodes (for relations), ObjectWithArgs nodes (for functions), or String values (for simple names)
- : List of AccessPriv nodes specifying the individual privileges; NIL indicates ALL PRIVILEGES
- : List of RoleSpec nodes identifying the roles receiving or losing the privileges
- : Boolean flag indicating whether the GRANT OPTION is being granted or revoked
- : RoleSpec identifying the role performing the grant (for privilege delegation scenarios)
- : DropBehavior enum (RESTRICT or CASCADE) controlling dependent privilege handling during REVOKE operations

## Dependencies
- Functions called/Symbols referenced:
  - [GrantTargetType](GrantTargetType.md)
  - ObjectType
  - [RoleSpec](../R/RoleSpec.md)
  - DropBehavior
- Called from (representative examples):
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md)
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [CreateCommandTag](../C/CreateCommandTag.md)

## Notes and Other Information
- Part of PostgreSQL's parse tree node system, inheriting from the standard Node structure
- Central to PostgreSQL's discretionary access control (DAC) security model
- The structure design enables batch operations on multiple objects and privileges simultaneously
- Supports PostgreSQL's role-based access control (RBAC) through the RoleSpec system
- Used in conjunction with the Access Control List (ACL) infrastructure for privilege enforcement
- The grant_option mechanism implements delegated administration capabilities
- CASCADE behavior during REVOKE can affect privileges granted by the target grantee to other roles
- Processed by the access control infrastructure in src/backend/catalog/aclchk.c