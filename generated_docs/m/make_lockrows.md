# make_lockrows

## Location
[src/backend/optimizer/plan/createplan.c:6940-6960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6940-L6960)

## Overview
Creates a LockRows plan node that applies row-level locking to tuples returned by its child plan, typically used in SELECT FOR UPDATE/SHARE queries.

## Definition

```c
static LockRows *
make_lockrows(Plan *lefttree, List *rowMarks, int epqParam)
```
## Detailed Description
The  function constructs a LockRows plan node that implements row-level locking functionality in PostgreSQL. This node is typically used to implement SELECT FOR UPDATE, SELECT FOR SHARE, and similar locking operations. The LockRows node processes tuples from its child plan and applies the specified locking semantics to each tuple before passing it up to the parent node.

The node maintains a list of row marking specifications that determine what type of locks to acquire and on which relations. It also handles Evaluation of Plan Quality (EPQ) parameters, which are used when concurrent updates require re-evaluation of the query plan to ensure consistent results.

## Parameters / Member Variables
- : The child plan node that provides tuples to be locked
- : List of PlanRowMark structures specifying locking requirements for different relations
- : Parameter ID used for EPQ (Evaluation of Plan Quality) re-evaluation when concurrent updates occur

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new LockRows node)
  - LockRows (the plan node type being created)
- Called from (representative examples):
  - [create_lockrows_plan](../c/create_lockrows_plan.md)
  - CP_IGNORE_TLIST

## Notes and Other Information
- The function is static and only used within createplan.c
- Sets  to NIL as LockRows nodes don't apply additional qualification conditions
- Sets  to NULL since LockRows nodes only have one child
- The target list is copied directly from the child plan since locking doesn't change the tuple structure
- Row marks specify different locking strengths (FOR UPDATE, FOR SHARE, FOR NO KEY UPDATE, FOR KEY SHARE)
- EPQ parameters are crucial for handling concurrent updates in a multi-user environment
- This node is essential for implementing PostgreSQL's MVCC (Multi-Version Concurrency Control) row locking semantics