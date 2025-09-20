# planstate_tree_walker_impl

## Location
[src/backend/nodes/nodeFuncs.c:4676-4683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L4676-L4683)

## Overview
This function traverses plan state trees by recursively visiting all sub-nodes of a given PlanState node, calling a user-provided callback function on each node.

## Definition

```c
bool
planstate_tree_walker_impl(PlanState *planstate,
						   planstate_tree_walker_callback walker,
						   void *context)
```
## Detailed Description
The  function implements a depth-first traversal of PostgreSQL plan state trees. It is designed to work after the current node has already been visited by the caller, so it only needs to recurse into sub-nodes. The function handles various types of plan nodes that can contain sub-plans or child plan states:

- **Init Plans**: Processes any initialization plans attached to the plan state
- **Left/Right Trees**: Visits outer and inner plan states (left and right subtrees)
- **Special Child Plans**: Handles specific node types with multiple child plans:
  - : Processes all append plans
  - : Processes all merge plans  
  - /: Processes all bitmap plans
  - : Processes the subquery plan
  - : Processes all custom scan plans
- **SubPlans**: Processes any subplans attached to the plan state

The function includes stack depth checking to prevent overflow from overly complex plan trees. It returns  if any callback returns  (indicating early termination), or  if the entire tree was traversed successfully.

## Parameters / Member Variables
- : The root PlanState node to start traversal from
- : Callback function of type  that will be called on each plan state node. The callback signature is: 
- : User-provided context data passed through to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  -  
  - 
  - 
- Called from (representative examples):
  -  (macro wrapper in src/include/nodes/nodeFuncs.h:180)
  - Used by functions in explain.c, execParallel.c, and execProcnode.c

## Notes and Other Information
- This is the implementation function for the  macro, which provides type-safe casting of the callback function
- The function uses a local macro  as shorthand for 
- Stack depth checking prevents infinite recursion in malformed or cyclic plan trees
- The traversal order is: initPlan → lefttree → righttree → special children → subPlan
- This function is part of PostgreSQL's plan state tree infrastructure used during query execution for operations like EXPLAIN, parallel query setup, and plan state management