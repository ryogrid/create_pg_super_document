# LockingClause

## Location
src/include/nodes/parsenodes.h: 831 - 837

## Overview
LockingClause represents the raw parse tree representation of FOR UPDATE/FOR SHARE locking clauses in SELECT statements, specifying which relations to lock and with what strength and wait policy.

## Definition


## Detailed Description
LockingClause captures the FOR UPDATE and FOR SHARE locking specifications found in SELECT statements. It represents the parsed form of row-level locking directives that control concurrent access to query results. The structure can specify particular relations to lock or apply to all relations in the query. It supports different locking strengths (UPDATE vs SHARE, with KEY variants) and wait policies (blocking, NOWAIT, or SKIP LOCKED) to handle lock conflicts.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : List of RangeVar nodes specifying relations to lock (NIL means all relations in query)
- : The type of lock to acquire (FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, or FOR KEY SHARE)
- : How to handle lock conflicts (block, NOWAIT, or SKIP LOCKED)

## Dependencies
- Functions called/Symbols referenced:
  - LockClauseStrength (enum defining lock strength types)
  - LockWaitPolicy (enum defining wait behavior on lock conflicts)
- Called from (representative examples):
  - transformSelectStmt (processes SELECT statement locking clauses)
  - transformLockingClause (transforms locking specifications)
  - transformSetOperationStmt (handles locking in UNION/INTERSECT/EXCEPT)
  - isLockedRefname (checks if a relation name is locked)

## Notes and Other Information
LockingClause is essential for PostgreSQL's row-level locking mechanism in SELECT statements. The lockedRels field uses RangeVar nodes primarily for their location information, and parse analysis requires unqualified relation names. When lockedRels is NIL, the locking applies to all relations in the query. The structure supports the full range of PostgreSQL's row locking options including the KEY variants introduced for foreign key optimization and the SKIP LOCKED feature for implementing work queues.