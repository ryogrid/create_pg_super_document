# CustomPathMethods

## Location
[src/include/nodes/extensible.h:92-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/extensible.h#L92-L106)

## Overview
CustomPathMethods defines the interface for custom path implementations in PostgreSQL's query planner, allowing extensions to provide specialized path generation and planning logic.

## Definition
```c
typedef struct CustomPathMethods
{
    const char *CustomName;

    /* Convert Path to a Plan */
    struct Plan *(*PlanCustomPath) (PlannerInfo *root,
                                   RelOptInfo *rel,
                                   struct CustomPath *best_path,
                                   List *tlist,
                                   List *clauses,
                                   List *custom_plans);
    struct List *(*ReparameterizeCustomPathByChild) (PlannerInfo *root,
                                                     List *custom_private,
                                                     RelOptInfo *child_rel);
} CustomPathMethods;
```

## Detailed Description
CustomPathMethods provides the callback interface for extensions to implement custom query execution paths within PostgreSQL's cost-based optimizer. These methods allow extensions to participate in query planning by providing specialized path generation logic and plan creation. The structure is referenced by CustomPath nodes and enables the planner to convert custom paths into executable plans while supporting advanced features like plan reparameterization for parameterized nested loops.

## Parameters / Member Variables
- `CustomName`: String identifier that uniquely identifies this custom path method implementation
- `PlanCustomPath`: Function pointer that converts a CustomPath into an executable Plan node, taking planner context, relation info, the best path chosen, target list, restriction clauses, and any child plans
- `ReparameterizeCustomPathByChild`: Function pointer for reparameterizing custom path data when a child relation changes, used in parameterized nested loop scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [CustomPath](CustomPath.md) (structure that uses these methods)
  - [Plan](../P/Plan.md) (return type for path planning)
  - [PlannerInfo](../P/PlannerInfo.md) (planner context)
  - [RelOptInfo](../R/RelOptInfo.md) (relation optimization info) 
  - [List](../L/List.md) (PostgreSQL list structure)
- Called from (representative examples):
  - [CustomPath](CustomPath.md) (structure references these methods)
  - [Query](../Q/Query.md) planner path conversion logic
  - Parameterized nested loop reparameterization

## Notes and Other Information
- Extensions register CustomPathMethods to enable custom query execution strategies
- The PlanCustomPath callback is mandatory and must convert the path representation into an actual executable plan
- ReparameterizeCustomPathByChild is optional and only needed for paths that support parameterization
- Custom paths allow extensions to implement specialized access methods, join algorithms, or optimization strategies
- The methods integrate with PostgreSQL's cost-based optimizer, allowing custom paths to compete with built-in paths based on estimated costs
- Extensions typically register these methods during initialization and reference them in CustomPath structures