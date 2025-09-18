# SQLFunctionCache

## Location
src/backend/executor/functions.c: 128 - 129

## Overview
SQLFunctionCache is a structure that caches parsed and planned SQL function data to avoid redundant parsing and planning operations during SQL function execution in PostgreSQL.

## Definition
```c
typedef struct
{
    char       *fname;          /* function name (for error msgs) */
    char       *src;            /* function body text (for error msgs) */
    
    SQLFunctionParseInfoPtr pinfo;  /* data for parser callback hooks */
    
    Oid         rettype;        /* actual return type */
    int16       typlen;         /* length of the return type */
    bool        typbyval;       /* true if return type is pass by value */
    bool        returnsSet;     /* true if returning multiple rows */
    bool        returnsTuple;   /* true if returning whole tuple result */
    bool        shutdown_reg;   /* true if registered shutdown callback */
    bool        readonly_func;  /* true to run in "read only" mode */
    bool        lazyEval;       /* true if using lazyEval for result query */
    
    ParamListInfo paramLI;      /* Param list representing current args */
    
    Tuplestorestate *tstore;    /* where we accumulate result tuples */
    
    JunkFilter *junkFilter;     /* will be NULL if function returns VOID */
    
    List       *func_state;     /* List of execution_state records */
    
    MemoryContext fcontext;     /* memory context holding this struct and all subsidiary data */
    
    LocalTransactionId lxid;    /* lxid in which cache was made */
    SubTransactionId subxid;    /* subxid in which cache was made */
} SQLFunctionCache;
```

## Detailed Description
SQLFunctionCache is a critical performance optimization structure used by PostgreSQL's SQL function execution system. It caches the results of parsing and planning SQL function bodies to avoid repeating these expensive operations on subsequent function calls. The cache is built during the first call to a SQL function and linked to the fn_extra field of the FmgrInfo struct.

The cache has a limited lifespan tied to the current transaction/subtransaction to ensure data consistency. The lxid and subxid fields track when the cache was created, and the cache is regenerated if these become obsolete. All cached data is stored in a dedicated memory context (fcontext) to enable clean cleanup when regeneration is needed.

## Parameters / Member Variables
- `fname`: Function name stored for error message generation
- `src`: Original function body text for error reporting
- `pinfo`: Parser callback hook data for handling function parameters
- `rettype`: OID of the function's actual return type  
- `typlen`: Length in bytes of the return type
- `typbyval`: Boolean indicating if return type is passed by value
- `returnsSet`: Boolean indicating if function returns multiple rows
- `returnsTuple`: Boolean indicating if function returns complete tuple results
- `shutdown_reg`: Boolean tracking if shutdown callback is registered
- `readonly_func`: Boolean indicating if function should run in read-only mode
- `lazyEval`: Boolean indicating if lazy evaluation is used for result queries
- `paramLI`: Parameter list information representing current function arguments
- `tstore`: Tuple store for accumulating result tuples
- `junkFilter`: Filter for removing junk columns (NULL for VOID-returning functions)
- `func_state`: List of execution_state records tracking query execution
- `fcontext`: Memory context containing this structure and all related data
- `lxid`: Local transaction ID when cache was created
- `subxid`: Subtransaction ID when cache was created

## Dependencies
- Functions called/Symbols referenced:
  - SQLFunctionParseInfoPtr
  - ParamListInfo  
  - Tuplestorestate
  - JunkFilter
  - List
  - MemoryContext
  - LocalTransactionId
  - SubTransactionId
- Referenced by:
  - SQLFunctionCachePtr (typedef pointer at src/backend/executor/functions.c:130)
  - init_sql_fcache (function at src/backend/executor/functions.c:615)

## Notes and Other Information
- Defined in src/backend/executor/functions.c:93-128
- Currently has only the lifespan of the calling query, though future improvements may extend this using plancache.c
- The cache data may physically survive across transactions when the FmgrInfo persists (particularly with indexes)
- Transaction safety is ensured by checking lxid/subxid and regenerating when obsolete
- All subsidiary data is kept in the fcontext memory context for efficient cleanup
- The func_state list maintains execution boundaries from original parse trees, with additional records chained via "next" fields for rule expansions