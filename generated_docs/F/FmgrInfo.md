# FmgrInfo

## Location
src/include/fmgr.h: 56 - 67

## Overview
FmgrInfo is a structure that holds system-catalog information required to call a function through the function manager, enabling efficient function call caching and reuse.

## Definition


## Detailed Description
FmgrInfo is a critical structure in PostgreSQL's function manager system that caches function metadata to avoid repeated system catalog lookups. When a function needs to be called multiple times, the system can perform the expensive catalog lookup once and store the results in an FmgrInfo structure for reuse. This optimization is essential for performance, especially in expression evaluation and aggregate processing where functions are called repeatedly. The structure contains all necessary information to dispatch function calls, including the actual function address, argument count, strictness properties, and optional handler-specific data.

## Parameters / Member Variables
- : Pointer to the actual function implementation or language handler
- : Object identifier of the function in the system catalog
- : Number of input arguments (limited by FUNC_MAX_ARGS)
- : Whether function follows strict semantics (NULL input produces NULL output)
- : Indicates if function returns a set of values
- : Statistics collection threshold for function execution tracking
- : Handler-specific extra data (modifiable by called function)
- : Memory context for storing fn_extra data
- : Parse tree node representing the function call expression

## Dependencies
- Functions called/Symbols referenced:
  - PGFunction (function pointer type)
  - Oid (object identifier type)
  - MemoryContext (memory management type)
  - fmNodePtr (parse tree node pointer)
- Called from (representative examples):
  - Used throughout the executor for function calls
  - Expression evaluation subsystem
  - Aggregate function processing
  - Procedural language handlers
  - Built-in function implementations

## Notes and Other Information
- Central to PostgreSQL's function call optimization strategy
- fn_extra field is specifically designed for function handlers to store state
- fn_expr contains parse-time information about arguments, not runtime values
- Enables efficient function call caching across multiple invocations
- Used by all function call mechanisms: direct calls, expression evaluation, aggregates
- Essential for performance in loops and repeated function evaluations
- The structure is typically initialized once and reused throughout query execution
- Memory management for fn_extra is handled through fn_mcxt to prevent leaks