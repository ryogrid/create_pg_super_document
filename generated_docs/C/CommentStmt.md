# CommentStmt

## Location
[src/include/nodes/parsenodes.h:3252-3258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3252-L3258)

## Overview
CommentStmt represents the parsed form of SQL COMMENT ON statements, which are used to add or remove comments on database objects in PostgreSQL.

## Definition

```c
typedef struct CommentStmt
{
	NodeTag		type;
	ObjectType	objtype;		/* Object's type */
	Node	   *object;			/* Qualified name of the object */
	char	   *comment;		/* Comment to insert, or NULL to remove */
} CommentStmt;
```
## Detailed Description
The CommentStmt structure is a parse tree node that encapsulates all information needed to execute a COMMENT ON SQL statement. It stores the type of database object being commented on, a reference to the object itself, and the comment text. When the comment field is NULL, it indicates that any existing comment should be removed from the object. This structure is used throughout PostgreSQL's command processing pipeline to handle comment operations on various database objects like tables, columns, functions, indexes, and other schema objects.

## Parameters / Member Variables
- : NodeTag identifying this as a CommentStmt parse node
- : ObjectType enum value specifying what kind of database object is being commented (table, function, index, etc.)
- : Node pointer to the qualified name representation of the target object
- : Character string containing the comment text to be stored, or NULL to remove existing comments

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enum for database object types)
  - Node (base parse tree node type)
  - NodeTag (parse node type identifier)
- Called from (representative examples):
  - CommentObject (comment.c:40)
  - standard_ProcessUtility (utility.c:1041)
  - ProcessUtilitySlow (utility.c:1805)
  - ATExecCmd (tablecmds.c:5343)

## Notes and Other Information
CommentStmt is processed by the utility command execution system and ultimately handled by CommentObject() function. The structure supports commenting on a wide variety of PostgreSQL objects including tables, columns, constraints, functions, operators, types, and many others. Comment operations are transactional and will be rolled back if the containing transaction fails.