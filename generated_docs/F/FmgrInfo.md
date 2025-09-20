# FmgrInfo

## Location
[src/include/fmgr.h:56-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fmgr.h#L56-L67)

## Overview
FmgrInfo is a structure that holds system-catalog information required to call a function through the function manager, enabling efficient function call caching and reuse.

## Definition

```c
typedef struct FmgrInfo
{
	PGFunction	fn_addr;		/* pointer to function or handler to be called */
	Oid			fn_oid;			/* OID of function (NOT of handler, if any) */
	short		fn_nargs;		/* number of input args (0..FUNC_MAX_ARGS) */
	bool		fn_strict;		/* function is "strict" (NULL in => NULL out) */
	bool		fn_retset;		/* function returns a set */
	unsigned char fn_stats;		/* collect stats if track_functions > this */
	void	   *fn_extra;		/* extra space for use by handler */
	MemoryContext fn_mcxt;		/* memory context to store fn_extra in */
	fmNodePtr	fn_expr;		/* expression parse tree for call, or NULL */
} FmgrInfo;
```
## Detailed Description
FmgrInfo is a critical structure in PostgreSQL's function manager system that caches function metadata to avoid repeated system catalog lookups. When a function needs to be called multiple times, the system can perform the expensive catalog lookup once and store the results in an FmgrInfo structure for reuse. This optimization is essential for performance, especially in expression evaluation and aggregate processing where functions are called repeatedly. The structure contains all necessary information to dispatch function calls, including the actual function address, argument count, strictness properties, and optional handler-specific data.

## Parameters / Member Variables
- `fn_addr`: Pointer to the actual function implementation or language handler
- `fn_oid`: Object identifier of the function in the system catalog
- `fn_nargs`: Number of input arguments (limited by FUNC_MAX_ARGS)
- `fn_strict`: Whether function follows strict semantics (NULL input produces NULL output)
- `fn_retset`: Indicates if function returns a set of values
- `fn_stats`: Statistics collection threshold for function execution tracking
- `*fn_extra`: Handler-specific extra data (modifiable by called function)
- `fn_mcxt`: Memory context for storing fn_extra data
- `fn_expr`: Parse tree node representing the function call expression
## Dependencies
- Functions called/Symbols referenced:
  - PGFunction (function pointer type)
  - Oid (object identifier type)
  - [MemoryContext](../M/MemoryContext.md) (memory management type)
  - [fmNodePtr](../f/fmNodePtr.md) (parse tree node pointer)
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