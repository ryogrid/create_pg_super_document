# record_plan_type_dependency

## Location
[src/backend/optimizer/plan/setrefs.c:3512-3552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3512-L3552)

## Overview
Records a dependency of the current query plan on a specific data type to enable proper plan invalidation when the type is modified.

## Definition
```c
void
record_plan_type_dependency(PlannerInfo *root, Oid typid)
```

## Detailed Description
This function is part of PostgreSQL's plan invalidation mechanism, specifically designed to track dependencies on data types (particularly domains). When query plan optimization removes or transforms type-related nodes (such as CoerceToDomain nodes during constant expression evaluation), the dependency on the underlying type must still be recorded to ensure proper plan invalidation.

The function works similarly to record_plan_function_dependency but targets type dependencies. It creates a PlanInvalItem that identifies the type using the TYPEOID syscache and stores it in the global planner state for later use by the plan caching system.

Currently, this function is primarily used by eval_const_expressions when it removes CoerceToDomain nodes during constant expression simplification, ensuring that the plan remains properly invalidated if the domain definition changes later.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the global planner state where dependency information is stored
- `typid`: OID of the data type (typically a domain) on which the plan depends

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant for built-in object threshold)
  - [PlanInvalItem](../P/PlanInvalItem.md) (structure type)
  - makeNode (node creation function)
  - GetSysCacheHashValue1 (syscache hash function)
  - [lappend](../l/lappend.md) (list append function)
- Called from (representative examples):
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)

## Notes and Other Information
- This is an exported function (not static) to allow eval_const_expressions to record type dependencies
- Uses TYPEOID syscache for tracking type dependencies, which plancache.c specifically expects
- Like record_plan_function_dependency, built-in types are not tracked for performance reasons
- Currently not called directly within setrefs.c, though future versions might call it from fix_expr_common
- Particularly important for domain types where constraints and definitions can change
- Part of the comprehensive plan invalidation system that ensures cached plans remain consistent with database object definitions
- Essential for maintaining correctness when domain definitions are altered after plans are cached

## Simplified Source

```c
void record_plan_type_dependency(PlannerInfo *root, Oid typid) {
    // Skip built-in types for performance (they never change)
    if (typid >= (Oid) FirstUnpinnedObjectId) {
        // Create plan invalidation item for this type
        PlanInvalItem *inval_item = makeNode(PlanInvalItem);

        // Use TYPEOID syscache to track the type dependency
        inval_item->cacheId = TYPEOID;
        inval_item->hashValue = GetSysCacheHashValue1(TYPEOID,
                                                     ObjectIdGetDatum(typid));

        // Add to global list of plan dependencies
        root->glob->invalItems = lappend(root->glob->invalItems, inval_item);
    }
}
```