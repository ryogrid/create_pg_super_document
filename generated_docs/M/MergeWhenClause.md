# MergeWhenClause

## Location
[src/include/nodes/parsenodes.h:1717-1727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1717-L1727)

## Overview
A structure representing a raw parser representation of a WHEN clause in a PostgreSQL MERGE statement, which is later transformed into MergeAction by parse analysis.

## Definition

```c
typedef struct MergeWhenClause
{
	NodeTag		type;
	MergeMatchKind matchKind;	/* MATCHED/NOT MATCHED BY SOURCE/TARGET */
	CmdType		commandType;	/* INSERT/UPDATE/DELETE/DO NOTHING */
	OverridingKind override;	/* OVERRIDING clause */
	Node	   *condition;		/* WHEN conditions (raw parser) */
	List	   *targetList;		/* INSERT/UPDATE targetlist */
	/* the following members are only used in INSERT actions */
	List	   *values;			/* VALUES to INSERT, or NULL */
} MergeWhenClause;
```
## Detailed Description
MergeWhenClause is a parse tree node that represents the syntactic structure of WHEN clauses within MERGE statements during the raw parsing phase. It captures the essential components of a WHEN clause including the match condition type (MATCHED vs NOT MATCHED), the command to execute (INSERT, UPDATE, DELETE, or DO NOTHING), and associated data like target lists and values. This structure serves as an intermediate representation that is later processed and transformed into executable MergeAction nodes during parse analysis.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : Specifies whether this clause handles MATCHED or NOT MATCHED cases, and by SOURCE or TARGET
- : The SQL command type to execute (INSERT, UPDATE, DELETE, or DO NOTHING)
- : Specifies the OVERRIDING clause behavior for identity/default columns
- : Pointer to the conditional expression node that determines when this clause applies
- : List of target expressions for INSERT/UPDATE operations
- : List of values to insert (used only for INSERT actions, NULL otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - MergeMatchKind
  - CmdType  
  - OverridingKind
- Called from (representative examples):
  - raw_expression_tree_walker_impl
  - setNamespaceForMergeWhen
  - transformMergeStmt

## Notes and Other Information
- This structure is part of the raw parse tree and represents the syntactic structure before semantic analysis
- The values member is specifically used only for INSERT actions and remains NULL for other command types
- During parse analysis, MergeWhenClause nodes are transformed into MergeAction nodes for execution
- Located in src/include/nodes/parsenodes.h at lines 1717-1727