# CachedPlanSource

## Location
[src/include/utils/plancache.h:96-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/plancache.h#L96-L135)

## Overview
CachedPlanSource represents a SQL query that is expected to be used multiple times, storing the query source text, raw parse tree, and analyzed-and-rewritten query tree along with adjunct data for cache invalidation handling.

## Definition
```c
typedef struct CachedPlanSource
{
    int             magic;              /* should equal CACHEDPLANSOURCE_MAGIC */
    struct RawStmt *raw_parse_tree;     /* output of raw_parser(), or NULL */
    const char     *query_string;       /* source text of query */
    CommandTag      commandTag;         /* command tag */
    Oid            *param_types;        /* array of parameter type OIDs, or NULL */
    int             num_params;         /* length of param_types array */
    ParserSetupHook parserSetup;        /* alternative parameter spec method */
    void           *parserSetupArg;
    int             cursor_options;     /* cursor options used for planning */
    bool            fixed_result;       /* disallow change in result tupdesc? */
    TupleDesc       resultDesc;         /* result type; NULL = doesn't return tuples */
    MemoryContext   context;            /* memory context holding all above */
    /* These fields describe the current analyzed-and-rewritten query tree: */
    List           *query_list;         /* list of Query nodes, or NIL if not valid */
    List           *relationOids;       /* OIDs of relations the queries depend on */
    List           *invalItems;         /* other dependencies, as PlanInvalItems */
    struct SearchPathMatcher *search_path; /* search_path used for parsing and planning */
    MemoryContext   query_context;      /* context holding the above, or NULL */
    Oid             rewriteRoleId;      /* Role ID we did rewriting for */
    bool            rewriteRowSecurity; /* row_security used during rewrite */
    bool            dependsOnRLS;       /* is rewritten query specific to the above? */
    /* If we have a generic plan, this is a reference-counted link to it: */
    struct CachedPlan *gplan;           /* generic plan, or NULL if not valid */
    /* Some state flags: */
    bool            is_oneshot;         /* is it a "oneshot" plan? */
    bool            is_complete;        /* has CompleteCachedPlan been done? */
    bool            is_saved;           /* has CachedPlanSource been "saved"? */
    bool            is_valid;           /* is the query_list currently valid? */
    int             generation;         /* increments each time we create a plan */
    /* If CachedPlanSource has been saved, it is a member of a global list */
    dlist_node      node;               /* list link, if is_saved */
    /* State kept to help decide whether to use custom or generic plans: */
    double          generic_cost;       /* cost of generic plan, or -1 if not known */
    double          total_custom_cost;  /* total cost of custom plans so far */
    int64           num_custom_plans;   /* # of custom plans included in total */
    int64           num_generic_plans;  /* # of generic plans */
} CachedPlanSource;
```

## Detailed Description
CachedPlanSource (which might better have been called CachedQuery) is a fundamental structure in PostgreSQL's plan caching system. It represents SQL queries expected to be executed multiple times and manages both the source representation and execution plans derived from it.

The structure supports cache invalidation through DDL operations affecting referenced objects. When invalidation occurs, the analyzed-and-rewritten query tree is discarded and rebuilt when next needed. The system can generate either generic plans (reusable with any parameters) or custom plans (optimized for specific parameter values).

CachedPlanSources have two memory contexts: one for the struct itself, query source text, and raw parse tree; another for the rewritten query tree and associated data. This design enables easy invalidation by discarding just the query context.

The system supports both saved plans (living for the backend lifetime) and unsaved plans (in transient storage), as well as "oneshot" variants for single-use queries requiring no data copying or invalidation checking.

## Parameters / Member Variables
- `magic`: Magic number for structure validation (CACHEDPLANSOURCE_MAGIC)
- `raw_parse_tree`: Output from raw_parser(), NULL for oneshot plans
- `query_string`: Original SQL query source text
- `commandTag`: Command tag identifying the SQL statement type
- `param_types`: Array of parameter type OIDs, NULL if no parameters
- `num_params`: Length of the param_types array
- `parserSetup`: Alternative hook for parameter specification
- `parserSetupArg`: Argument for parserSetup hook
- `cursor_options`: Cursor options used during planning
- `fixed_result`: Whether to disallow changes in result tuple descriptor
- `resultDesc`: Result tuple descriptor, NULL for non-tuple-returning queries
- `context`: Memory context containing the above fields
- `query_list`: List of analyzed Query nodes, NIL if invalid
- `relationOids`: OIDs of relations the queries depend on
- `invalItems`: Other dependencies as PlanInvalItems
- `search_path`: Search path used for parsing and planning
- `query_context`: Memory context for query tree, NULL if invalid
- `rewriteRoleId`: Role ID used during query rewriting
- `rewriteRowSecurity`: Whether row security was enabled during rewrite
- `dependsOnRLS`: Whether rewritten query is role-specific
- `gplan`: Reference-counted link to generic CachedPlan
- `is_oneshot`: Whether this is a oneshot plan
- `is_complete`: Whether CompleteCachedPlan has been called
- `is_saved`: Whether the CachedPlanSource has been saved
- `is_valid`: Whether query_list is currently valid
- `generation`: Counter incremented each time a plan is created
- `node`: List link for saved CachedPlanSources global list
- `generic_cost`: Cost of generic plan, -1 if unknown
- `total_custom_cost`: Total cost of all custom plans generated
- `num_custom_plans`: Number of custom plans included in total cost
- `num_generic_plans`: Number of generic plans generated

## Dependencies
- Functions called/Symbols referenced:
  - [RawStmt](../R/RawStmt.md)
  - CommandTag
  - [SearchPathMatcher](../S/SearchPathMatcher.md)
  - [CachedPlan](CachedPlan.md)
  - [dlist_node](../d/dlist_node.md)

- Called from (representative examples):
  - [PrepareQuery](../P/PrepareQuery.md) (src/backend/commands/prepare.c:60)
  - [SPI_keepplan](../S/SPI_keepplan.md) (src/backend/executor/spi.c:994)
  - [exec_parse_message](../e/exec_parse_message.md) (src/backend/tcop/postgres.c:1405)
  - [CreateCachedPlan](CreateCachedPlan.md) (src/backend/utils/cache/plancache.c:196)
  - [GetCachedPlan](../G/GetCachedPlan.md) (src/backend/utils/cache/plancache.c:1168)

## Notes and Other Information
- [CachedPlanSource](CachedPlanSource.md) serves as the foundation for PostgreSQL's prepared statement and plan caching infrastructure
- The structure maintains statistics to help decide between custom and generic plan usage
- Invalidation events automatically trigger recompilation of dependent plans
- Memory management is carefully designed with separate contexts for different lifecycle components
- The commandTag field is assumed to reference a compile-time constant string
- Oneshot plans live entirely in the caller's CurrentMemoryContext and cannot be freed independently