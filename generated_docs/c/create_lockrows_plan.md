# create_lockrows_plan

## Location
[src/backend/optimizer/plan/createplan.c:2792-2814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2792-L2814)

## Overview
Creates a LockRows plan node that implements row-level locking operations, typically used for SELECT FOR UPDATE/SHARE queries and other locking scenarios.

## Definition

```c
static LockRows *
create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path,
					 int flags)
```
## Detailed Description
This function constructs a LockRows plan node that handles row-level locking in PostgreSQL queries. The LockRows operation is used to implement various locking modes like SELECT FOR UPDATE, SELECT FOR SHARE, and similar constructs that need to acquire locks on specific rows during query execution. The function creates a plan for the underlying subpath and configures the LockRows node with the appropriate row marks and evaluation-time parameters needed for proper lock acquisition and management.

Since LockRows doesn't modify the target list, it simply passes through the tlist requirements from its parent operation to its subplan without modification.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and information for plan generation
- : LockRowsPath representing the chosen execution strategy for the locking operation, containing subpath, row marks, and evaluation parameters
- : Integer flags controlling plan creation behavior, passed through unchanged to the subplan

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [make_lockrows](../m/make_lockrows.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c module
- [LockRows](../L/LockRows.md) operations don't project new columns, so target list requirements pass through unchanged
- Row marks (best_path->rowMarks) specify the type and strength of locks to be acquired
- The epqParam (evaluation-time parameter) is used for handling concurrent updates in isolation
- Essential for implementing PostgreSQL's row-level locking mechanisms
- Used primarily for SELECT FOR UPDATE, SELECT FOR SHARE, and similar locking constructs
- The plan node handles lock acquisition during query execution, not during planning

## Simplified Source

```c
static LockRows *
create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path, int flags)
{
    LockRows *plan;
    Plan *subplan;

    // LockRows doesn't project, so tlist requirements pass through
    subplan = create_plan_recurse(root, best_path->subpath, flags);

    // Create LockRows plan with row marks and evaluation parameters
    plan = make_lockrows(subplan, best_path->rowMarks, best_path->epqParam);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}
```