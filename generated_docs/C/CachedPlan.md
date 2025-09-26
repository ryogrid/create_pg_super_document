# CachedPlan

## Location
[src/include/utils/plancache.h:147-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/plancache.h#L147-L161)

## Overview
CachedPlan represents an execution plan derived from a CachedPlanSource, with reference counting to manage the lifecycle of both the parent link and active plan executions.

## Definition
```c
typedef struct CachedPlan
{
    int             magic;          /* should equal CACHEDPLAN_MAGIC */
    List           *stmt_list;      /* list of PlannedStmts */
    bool            is_oneshot;     /* is it a "oneshot" plan? */
    bool            is_saved;       /* is CachedPlan in a long-lived context? */
    bool            is_valid;       /* is the stmt_list currently valid? */
    Oid             planRoleId;     /* Role ID the plan was created for */
    bool            dependsOnRole;  /* is plan specific to that role? */
    TransactionId   saved_xmin;     /* if valid, replan when TransactionXmin changes from this value */
    int             generation;     /* parent's generation number for this plan */
    int             refcount;       /* count of live references to this struct */
    MemoryContext   context;        /* context containing this CachedPlan */
} CachedPlan;
```

## Detailed Description
CachedPlan represents an execution plan derived from a CachedPlanSource and serves as the executable form of cached queries in PostgreSQL. The structure is reference-counted, allowing it to be shared across multiple executions and automatically discarded when no longer needed.

The plan contains a list of PlannedStmts that represent the optimized execution strategy for the query. Plans can be either generic (parameterizable and reusable) or custom (optimized for specific parameter values). All CachedPlans must be treated as read-only when shared across executions.

The structure includes role-specific information since plans may be optimized differently based on the executing role's permissions and row-level security settings. Transaction-specific validation ensures plans remain valid across transaction boundaries.

Memory management is handled through a dedicated context, making cleanup straightforward when the plan is no longer referenced. For oneshot plans, the context is shared with the caller and cannot be independently freed.

## Parameters / Member Variables
- `magic`: Magic number for structure validation (CACHEDPLAN_MAGIC)
- `stmt_list`: List of PlannedStmt nodes representing the execution plan
- `is_oneshot`: Whether this is a oneshot plan with shared memory context
- `is_saved`: Whether the plan resides in a long-lived memory context
- `is_valid`: Whether the stmt_list is currently valid for execution
- `planRoleId`: OID of the role for which this plan was created
- `dependsOnRole`: Whether the plan is specific to the creating role
- `saved_xmin`: Transaction ID for replan validation when TransactionXmin changes
- `generation`: Generation number from parent CachedPlanSource for this plan
- `refcount`: Reference count tracking live pointers to this structure
- `context`: Memory context containing this CachedPlan and associated data

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references in the struct definition)

- Called from (representative examples):
  - [ExecuteQuery](../E/ExecuteQuery.md) (src/backend/commands/prepare.c:153)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (src/backend/executor/spi.c:1581)
  - [exec_bind_message](../e/exec_bind_message.md) (src/backend/tcop/postgres.c:1640)
  - [GetCachedPlan](../G/GetCachedPlan.md) (src/backend/utils/cache/plancache.c:1171)
  - [ReleaseCachedPlan](../R/ReleaseCachedPlan.md) (src/backend/utils/cache/plancache.c:1291)

## Notes and Other Information
- CachedPlans are automatically discarded when their reference count reaches zero
- Plans can outlive their originating CachedPlanSource due to reference counting
- Reference counting ensures safe sharing across multiple concurrent executions
- The generation field helps track plan validity relative to the parent CachedPlanSource
- Transaction-based validation prevents execution of stale plans across transaction boundaries
- Role-dependent plans ensure proper security isolation in multi-user environments
- Memory context management enables efficient cleanup of no-longer-needed execution plans