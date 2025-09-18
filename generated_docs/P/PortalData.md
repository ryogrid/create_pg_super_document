# PortalData

## Location
[src/include/utils/portal.h:115-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/portal.h#L115-L206)

## Overview
PortalData is a comprehensive structure that encapsulates all information needed for executing and managing SQL queries in PostgreSQL, including query state, execution context, parameters, and result handling.

## Definition
```c
typedef struct PortalData
{
    /* Bookkeeping data */
    const char *name;               /* portal's name */
    const char *prepStmtName;       /* source prepared statement (NULL if none) */
    MemoryContext portalContext;    /* subsidiary memory for portal */
    ResourceOwner resowner;         /* resources owned by portal */
    void        (*cleanup) (Portal portal); /* cleanup hook */

    /* State data for remembering which subtransaction(s) the portal was created or used in */
    SubTransactionId createSubid;   /* the creating subxact */
    SubTransactionId activeSubid;   /* the last subxact with activity */
    int         createLevel;        /* creating subxact's nesting level */

    /* The query or queries the portal will execute */
    const char *sourceText;         /* text of query (as of 8.4, never NULL) */
    CommandTag  commandTag;         /* command tag for original query */
    QueryCompletion qc;             /* command completion data for executed query */
    List       *stmts;              /* list of PlannedStmts */
    CachedPlan *cplan;              /* CachedPlan, if stmts are from one */

    ParamListInfo portalParams;     /* params to pass to query */
    QueryEnvironment *queryEnv;     /* environment for query */

    /* Features/options */
    PortalStrategy strategy;        /* see above */
    int         cursorOptions;      /* DECLARE CURSOR option bits */
    bool        run_once;           /* unused */

    /* Status data */
    PortalStatus status;            /* see above */
    bool        portalPinned;       /* a pinned portal can't be dropped */
    bool        autoHeld;           /* was automatically converted from pinned to held */

    /* If not NULL, Executor is active; call ExecutorEnd eventually: */
    QueryDesc  *queryDesc;          /* info needed for executor invocation */

    /* If portal returns tuples, this is their tupdesc: */
    TupleDesc   tupDesc;            /* descriptor for result tuples */
    int16      *formats;            /* a format code for each column */

    /* Outermost ActiveSnapshot for execution of the portal's queries */
    Snapshot    portalSnapshot;     /* active snapshot, or NULL if none */

    /* Where we store tuples for a held cursor or specific portal types */
    Tuplestorestate *holdStore;     /* store for holdable cursors */
    MemoryContext holdContext;      /* memory containing holdStore */

    /* Snapshot under which tuples in the holdStore were read */
    Snapshot    holdSnapshot;       /* registered snapshot, or NULL if none */

    /* Current cursor position indicators */
    bool        atStart;
    bool        atEnd;
    uint64      portalPos;

    /* Presentation data, primarily used by the pg_cursors system view */
    TimestampTz creation_time;      /* time at which this portal was defined */
    bool        visible;            /* include this portal in pg_cursors? */
} PortalData;
```

## Detailed Description
PortalData is the core data structure that represents a query execution context in PostgreSQL. It maintains all the necessary information for executing SQL statements from initial parsing through completion. The structure is designed to handle various types of queries including simple statements, prepared statements, and cursors. It manages the complete execution lifecycle including memory allocation, transaction state tracking, parameter binding, result formatting, and cleanup operations. The structure supports sophisticated features like holdable cursors that can persist across transaction boundaries and provides detailed execution state tracking for proper resource management.

## Parameters / Member Variables
- `name`: Unique identifier string for the portal
- `prepStmtName`: Name of the source prepared statement if this portal was created from one
- `portalContext`: Dedicated memory context for portal-specific allocations
- `resowner`: Resource owner for tracking and cleanup of portal resources
- `cleanup`: Function pointer for custom cleanup operations
- `createSubid`: Subtransaction ID where the portal was created
- `activeSubid`: Subtransaction ID where the portal was last used
- `createLevel`: Nesting level of the creating subtransaction
- `sourceText`: Original SQL text of the query
- `commandTag`: Command type identifier for the original query
- `qc`: Query completion information including row counts
- `stmts`: List of planned statements ready for execution
- `cplan`: Reference to cached plan if statements came from plan cache
- `portalParams`: Parameter values to be bound to the query
- `queryEnv`: Query execution environment and settings
- `strategy`: Execution strategy (ONE_SELECT, MULTI_QUERY, etc.)
- `cursorOptions`: Option bits for cursor behavior (scrollable, hold, etc.)
- `run_once`: Flag indicating single-use portal (currently unused)
- `status`: Current execution state (NEW, DEFINED, READY, ACTIVE, DONE, FAILED)
- `portalPinned`: Flag preventing portal deletion while pinned
- `autoHeld`: Flag indicating automatic conversion from pinned to held
- `queryDesc`: Active query descriptor for executor operations
- `tupDesc`: Tuple descriptor for result set structure
- `formats`: Array of format codes for each result column
- `portalSnapshot`: Snapshot for consistent query execution
- `holdStore`: Tuple store for holding cursor results
- `holdContext`: Memory context for held tuple storage
- `holdSnapshot`: Snapshot for accessing held tuples
- `atStart`: Flag indicating cursor is at start position
- `atEnd`: Flag indicating cursor is at end position
- `portalPos`: Current cursor position (row number)
- `creation_time`: Timestamp when portal was created
- `visible`: Flag for inclusion in pg_cursors system view

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner
  - [Portal](Portal.md) (for cleanup function pointer)
  - SubTransactionId
  - CommandTag
  - QueryCompletion
  - CachedPlan
  - [ParamListInfo](ParamListInfo.md)
  - QueryEnvironment
  - PortalStrategy
  - [PortalStatus](PortalStatus.md)
  - QueryDesc
  - TuplestoreState
- Called from (representative examples):
  - [Portal](Portal.md) typedef (as the underlying structure)
  - [Portal](Portal.md) management functions throughout the codebase

## Notes and Other Information
- [PortalData](PortalData.md) is the fundamental structure underlying all portal operations in PostgreSQL
- Supports complex transaction semantics with subtransaction tracking
- Memory management is carefully designed with dedicated contexts for different data lifetimes
- The structure handles both simple query execution and complex cursor operations
- [Snapshot](../S/Snapshot.md) management ensures consistent reads for long-running operations
- Position tracking enables bidirectional cursor movement and precise result set navigation
- The cleanup mechanism allows for proper resource deallocation and custom cleanup operations
- Integration with the resource owner system ensures proper cleanup on transaction abort
- Supports both temporary and persistent (holdable) result storage
- The visible flag enables selective exposure of portals to system monitoring views