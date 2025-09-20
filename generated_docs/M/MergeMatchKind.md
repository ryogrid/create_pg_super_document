# MergeMatchKind

## Location
[src/include/nodes/primnodes.h:1999-2000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1999-L2000)

## Overview
An enumeration type that defines the different match conditions for WHEN clauses in PostgreSQL MERGE statements, specifying whether a row is matched, not matched by source, or not matched by target.

## Definition

```c
typedef struct MergeAction
{
	NodeTag		type;
	MergeMatchKind matchKind;	/* MATCHED/NOT MATCHED BY SOURCE/TARGET */
	CmdType		commandType;	/* INSERT/UPDATE/DELETE/DO NOTHING */
	/* OVERRIDING clause */
	OverridingKind override pg_node_attr(query_jumble_ignore);
	Node	   *qual;			/* transformed WHEN conditions */
	List	   *targetList;		/* the target list (of TargetEntry) */
	/* target attribute numbers of an UPDATE */
	List	   *updateColnos pg_node_attr(query_jumble_ignore);
} MergeAction;
```
## Detailed Description
MergeMatchKind is a fundamental enumeration used in PostgreSQL's MERGE statement implementation to categorize the three distinct matching scenarios that can occur when merging data from a source table into a target table. This enum is used throughout both the parser and execution phases to distinguish between different WHEN clause conditions.

The enum values correspond to the SQL standard MERGE statement syntax:
- : Represents rows that exist in both source and target tables (matching join condition)
- : Represents rows that exist only in the target table (no corresponding source row)  
- : Represents rows that exist only in the source table (no corresponding target row)

This enumeration is central to the MERGE statement's conditional logic, allowing different actions (INSERT, UPDATE, DELETE, DO NOTHING) to be applied based on the match status of each row.

## Parameters / Member Variables
- `MERGE_WHEN_MATCHED`: Indicates a row exists in both source and target tables based on the merge join condition
- `MERGE_WHEN_NOT_MATCHED_BY_SOURCE`: Indicates a row exists in the target table but has no matching row in the source table
- `MERGE_WHEN_NOT_MATCHED_BY_TARGET`: Indicates a row exists in the source table but has no matching row in the target table

## Dependencies
- Functions called/Symbols referenced: (None - this is a basic enum type)
- Used by:
  -  (in parser nodes for raw MERGE statement representation)
  -  (in execution nodes for transformed MERGE statement representation)

## Notes and Other Information
- The constant  is defined as  to provide the total count of enum values
- This enum bridges the gap between SQL MERGE syntax and PostgreSQL's internal execution representation
- The transformation from  (parser representation) to  (execution representation) preserves the  value to maintain the semantic meaning of the WHEN condition
- Located at src/include/nodes/primnodes.h:1994-1999