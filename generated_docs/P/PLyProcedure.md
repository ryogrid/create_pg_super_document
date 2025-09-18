# PLyProcedure

## Location
src/pl/plpython/plpy_procedure.h: 25 - 50

## Overview
PLyProcedure is the central structure in PostgreSQL's PL/Python extension that caches compiled procedure data, managing all aspects of a Python function including its source code, argument handling, execution context, and state management across calls.

## Definition
```c
typedef struct PLyProcedure
{
    MemoryContext mcxt;         /* context holding this PLyProcedure and its subsidiary data */
    char       *proname;        /* SQL name of procedure */
    char       *pyname;         /* Python name of procedure */
    TransactionId fn_xmin;
    ItemPointerData fn_tid;
    bool        fn_readonly;
    bool        is_setof;       /* true, if function returns result set */
    bool        is_procedure;
    bool        is_trigger;     /* called as trigger? */
    PLyObToDatum result;        /* Function result output conversion info */
    PLyDatumToOb result_in;     /* For converting input tuples in a trigger */
    char       *src;            /* textual procedure code, after mangling */
    char      **argnames;       /* Argument names */
    PLyDatumToOb *args;         /* Argument input conversion info */
    int         nargs;          /* Number of elements in above arrays */
    Oid         langid;         /* OID of plpython pg_language entry */
    List       *trftypes;       /* OID list of transform types */
    PyObject   *code;           /* compiled procedure code */
    PyObject   *statics;        /* data saved across calls, local scope */
    PyObject   *globals;        /* data saved across calls, global scope */
    long        calldepth;      /* depth of recursive calls of function */
    PLySavedArgs *argstack;     /* stack of outer-level call arguments */
} PLyProcedure;
```

## Detailed Description
PLyProcedure serves as the comprehensive cache and execution context for PL/Python functions in PostgreSQL. It maintains the complete lifecycle of a Python procedure from compilation to execution, handling argument conversion, result processing, and state management. The structure supports various function types including regular functions, set-returning functions, triggers, and procedures. It manages memory contexts, tracks transaction information for cache invalidation, and provides recursion support through argument stacking. The structure also handles type conversion between PostgreSQL and Python data types bidirectionally.

## Parameters / Member Variables
- `mcxt`: Memory context that holds this PLyProcedure structure and all its subsidiary data
- `proname`: SQL name of the procedure as defined in PostgreSQL
- `pyname`: Python name of the procedure used in the Python execution environment
- `fn_xmin`: Transaction ID for cache invalidation tracking
- `fn_tid`: Item pointer for cache invalidation tracking
- `fn_readonly`: Boolean indicating if the function is read-only
- `is_setof`: Boolean flag indicating if the function returns a result set
- `is_procedure`: Boolean indicating if this is a procedure (vs function)
- `is_trigger`: Boolean flag indicating if this is called as a trigger function
- `result`: Structure containing function result output conversion information
- `result_in`: Structure for converting input tuples in trigger functions
- `src`: Textual procedure code after Python-specific mangling and processing
- `argnames`: Array of argument names as strings
- `args`: Array of structures for converting PostgreSQL arguments to Python objects
- `nargs`: Number of elements in the argnames and args arrays
- `langid`: OID of the plpython entry in pg_language system catalog
- `trftypes`: List of OIDs representing transform types for the function
- `code`: Compiled Python bytecode object ready for execution
- `statics`: Python dictionary for data persistence across calls (local scope)
- `globals`: Python dictionary for data persistence across calls (global scope)
- `calldepth`: Current depth of recursive function calls for stack management
- `argstack`: Linked list stack of saved arguments for recursive calls

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (for result output conversion)
  - [PLyDatumToOb](PLyDatumToOb.md) (for argument input conversion and trigger result input)
  - [PLySavedArgs](PLySavedArgs.md) (for argument stack management)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (executes regular functions)
  - [PLy_exec_trigger](PLy_exec_trigger.md) (executes trigger functions)
  - [PLy_procedure_get](PLy_procedure_get.md) (retrieves cached procedures)
  - [PLy_procedure_create](PLy_procedure_create.md) (creates new procedure instances)
  - [PLy_procedure_compile](PLy_procedure_compile.md) (compiles procedure code)
  - [PLy_procedure_delete](PLy_procedure_delete.md) (removes procedures from cache)
  - [PLy_procedure_valid](PLy_procedure_valid.md) (validates cached procedures)
  - [PLyExecutionContext](PLyExecutionContext.md) (execution context management)
  - [PLyProcedureEntry](PLyProcedureEntry.md) (procedure cache entry management)

## Notes and Other Information
- This structure is the cornerstone of PL/Python's caching mechanism, avoiding recompilation on each call
- Memory management is critical as it holds Python objects that must be properly reference-counted
- The structure supports complex scenarios including recursion, set-returning functions, and triggers
- Cache invalidation is handled through transaction tracking (fn_xmin, fn_tid)
- The argstack mechanism enables proper argument isolation during recursive calls
- Transform types (trftypes) allow for custom data type conversions between PostgreSQL and Python