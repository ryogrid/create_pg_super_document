# jsonb_exec_setup

## Location
[src/backend/utils/adt/jsonbsubs.c:353-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L353-L401)

## Overview
Sets up execution state for a JSONB subscript operation, preparing workspace and method pointers for accessing JSONB data through subscripting syntax.

## Definition

```c
static void
jsonb_exec_setup(const SubscriptingRef *sbsref,
				 SubscriptingRefState *sbsrefstate,
				 SubscriptExecSteps *methods)
```
## Detailed Description
This function initializes the execution state for JSONB subscript operations (e.g., ). Unlike array subscripting which has nesting limits, JSONB subscripting has no inherent nesting limitations since the JSONB type itself doesn't impose such restrictions.

The function allocates a type-specific workspace () that includes space for per-subscript data, collects subscript data types needed during execution, and sets up method pointers for the actual subscript operations. The workspace is carefully laid out in memory with proper alignment considerations.

## Parameters / Member Variables
- `*sbsref`: Pointer to SubscriptingRef structure containing the subscript reference information including upper index expressions
- `*sbsrefstate`: Pointer to SubscriptingRefState structure where the allocated workspace will be stored
- `*methods`: Pointer to SubscriptExecSteps structure that will be populated with function pointers for subscript operations
## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (memory alignment macro)
  -  (list cell access)
  -  (list iteration helper)
  -  (expression type determination)
  - 
  - 
  - 
  - 
- Data structures referenced:
  - 
  -   
  - 
  - 
- Called from:
  - 

## Notes and Other Information
- The function is static (internal to jsonbsubs.c file)
- Memory allocation includes careful alignment calculations to ensure proper pointer alignment
- The workspace expectArray field is set to false, distinguishing JSONB subscripting from array subscripting
- The function assumes  for proper memory alignment
- Located in

## Simplified Source

```c
static void jsonb_exec_setup(const SubscriptingRef *sbsref,
                            SubscriptingRefState *sbsrefstate,
                            SubscriptExecSteps *methods) {
    JsonbSubWorkspace *workspace;
    ListCell *lc;
    int nupper = sbsref->refupperindexpr->length;
    char *ptr;

    // Allocate workspace with space for per-subscript data
    workspace = palloc0(MAXALIGN(sizeof(JsonbSubWorkspace)) +
                       nupper * (sizeof(Datum) + sizeof(Oid)));
    workspace->expectArray = false;
    ptr = ((char *) workspace) + MAXALIGN(sizeof(JsonbSubWorkspace));

    // Set up workspace arrays with proper alignment
    workspace->index = (Datum *) ptr;
    ptr += nupper * sizeof(Datum);
    workspace->indexOid = (Oid *) ptr;

    sbsrefstate->workspace = workspace;

    // Collect subscript data types for execution
    foreach(lc, sbsref->refupperindexpr) {
        Node *expr = lfirst(lc);
        int i = foreach_current_index(lc);
        workspace->indexOid[i] = exprType(expr);
    }

    // Set up method function pointers
    methods->sbs_check_subscripts = jsonb_subscript_check_subscripts;
    methods->sbs_fetch = jsonb_subscript_fetch;
    methods->sbs_assign = jsonb_subscript_assign;
    methods->sbs_fetch_old = jsonb_subscript_fetch_old;
}
``` 