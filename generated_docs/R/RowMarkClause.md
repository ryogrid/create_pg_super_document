# RowMarkClause

## Location
src/include/nodes/parsenodes.h: 1576 - 1583

## Overview
RowMarkClause represents the parser output for FOR UPDATE, FOR SHARE, FOR KEY UPDATE, and FOR KEY SHARE clauses, specifying row-level locking requirements for specific relations in a query.

## Definition


## Detailed Description
RowMarkClause nodes are created for each relation that is targeted by a FOR UPDATE/SHARE clause. Each node specifies the type of lock to acquire, the wait policy for handling conflicts, and whether the locking clause was explicitly written at the current query level or pushed down from a higher level.

When a locking clause is applied to a subquery, PostgreSQL generates RowMarkClause nodes for all normal and subquery relations within that subquery, marking them with pushedDown = true to distinguish them from explicitly written clauses. This mechanism ensures proper lock propagation through query hierarchies.

The strength field determines the lock level (ranging from FOR KEY SHARE to FOR UPDATE), while waitPolicy controls behavior when encountering locked rows (normal wait, NOWAIT, or SKIP LOCKED). The Query.hasForUpdate flag separately tracks whether explicit FOR UPDATE/SHARE clauses exist at the current query level.

## Parameters / Member Variables
- : NodeTag identifying this as a RowMarkClause node
- : Range table index identifying the target relation for locking
- : LockClauseStrength enumeration specifying the lock level (FOR KEY SHARE, FOR SHARE, FOR NO KEY UPDATE, FOR UPDATE)
- : LockWaitPolicy enumeration controlling lock conflict handling (normal wait, NOWAIT, SKIP LOCKED)
- : Boolean indicating whether this clause was pushed down from a higher query level rather than explicitly written

## Dependencies
- Functions called/Symbols referenced:
  - LockClauseStrength (enumeration for lock strength levels)
  - LockWaitPolicy (enumeration for wait policies)
  - Index (for range table references)
- Called from (representative examples):
  - applyLockingClause (parser/analyze.c)
  - preprocess_rowmarks (optimizer/plan/planner.c)
  - transformDeclareCursorStmt (parser/analyze.c)
  - get_parse_rowmark (parser/parse_relation.c)

## Notes and Other Information
- Query.rowMarks contains separate RowMarkClause nodes for each relation requiring locking
- Higher numerical values in LockClauseStrength and LockWaitPolicy take precedence when a relation is specified multiple ways
- The pushedDown mechanism enables proper lock inheritance in subqueries while maintaining distinction from explicit clauses
- Lock strength ordering allows the system to automatically choose the strongest required lock when multiple specifications exist
- Used in conjunction with Query.hasForUpdate to track explicit locking requirements at each query level