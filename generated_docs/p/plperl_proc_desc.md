# plperl_proc_desc

## Location
[src/pl/plperl/plperl.c:100-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L100-L125)

## Overview
The plperl_proc_desc structure represents cached information about loaded Perl procedures in PostgreSQL's PL/Perl implementation. It manages the lifecycle, metadata, and execution context for compiled Perl functions.

## Definition

```c
typedef struct plperl_proc_desc
{
	char	   *proname;		/* user name of procedure */
	MemoryContext fn_cxt;		/* memory context for this procedure */
	unsigned long fn_refcount;	/* number of active references */
	TransactionId fn_xmin;		/* xmin/TID of procedure's pg_proc tuple */
	ItemPointerData fn_tid;
	SV		   *reference;		/* CODE reference for Perl sub */
	plperl_interp_desc *interp; /* interpreter it's created in */
	bool		fn_readonly;	/* is function readonly (not volatile)? */
	Oid			lang_oid;
	List	   *trftypes;
	bool		lanpltrusted;	/* is it plperl, rather than plperlu? */
	bool		fn_retistuple;	/* true, if function returns tuple */
	bool		fn_retisset;	/* true, if function returns set */
	bool		fn_retisarray;	/* true if function returns array */
	/* Conversion info for function's result type: */
	Oid			result_oid;		/* Oid of result type */
	FmgrInfo	result_in_func; /* I/O function and arg for result type */
	Oid			result_typioparam;
	/* Per-argument info for function's argument types: */
	int			nargs;
	FmgrInfo   *arg_out_func;	/* output fns for arg types */
	bool	   *arg_is_rowtype; /* is each arg composite? */
	Oid		   *arg_arraytype;	/* InvalidOid if not an array */
} plperl_proc_desc;
```
## Detailed Description
This structure caches comprehensive information about compiled Perl procedures to avoid recompilation on subsequent calls. It employs reference counting to manage the lifetime of the cached data - the structure and its associated Perl subroutine are released when fn_refcount reaches zero. Memory management is handled through the dedicated fn_cxt memory context, which automatically cleans up all subsidiary data when deleted.

The structure tracks both metadata (function name, type information, flags) and execution context (interpreter reference, compiled Perl code reference). It supports various function types including regular functions, set-returning functions, and functions that return arrays or tuples.

## Parameters / Member Variables
- `*proname`: User-visible name of the procedure
- `fn_cxt`: Dedicated memory context for managing this procedure's memory allocations
- `fn_refcount`: Reference counter tracking active usage (hash table + active call levels)
- `fn_xmin`: Transaction ID from the procedure's pg_proc tuple for cache invalidation
- `fn_tid`: Item pointer data for the procedure's pg_proc tuple
- `*reference`: Perl CODE reference (SV*) pointing to the compiled Perl subroutine
- `*interp`: Pointer to the plperl_interp_desc containing the interpreter where this function was compiled
- `fn_readonly`: Flag indicating if function is readonly (non-volatile)
- `lang_oid`: OID of the procedural language (plperl vs plperlu)
- `*trftypes`: List of transform types for the function
- `lanpltrusted`: Boolean indicating trusted (plperl) vs untrusted (plperlu) context
- `fn_retistuple`: Flag for functions returning composite types
- `fn_retisset`: Flag for set-returning functions
- `fn_retisarray`: Flag for functions returning arrays
- `result_oid`: OID of the function's return type
- `result_in_func`: Input function information for result type conversion
- `result_typioparam`: Type-specific parameter for result conversion
- `nargs`: Number of function arguments
- `*arg_out_func`: Array of output functions for converting argument types
- `*arg_is_rowtype`: Array of flags indicating which arguments are composite types
- `*arg_arraytype`: Array of OIDs for array element types (InvalidOid for non-arrays)
## Dependencies
- Functions called/Symbols referenced:
  - [plperl_interp_desc](plperl_interp_desc.md) (interpreter reference)
- Called from (representative examples):
  - [compile_plperl_function](../c/compile_plperl_function.md) (function compilation)
  - [plperl_call_perl_func](plperl_call_perl_func.md) (function execution)
  - [plperl_trigger_handler](plperl_trigger_handler.md) (trigger execution)
  - [free_plperl_function](../f/free_plperl_function.md) (cleanup)
  - [validate_plperl_function](../v/validate_plperl_function.md) (validation)

## Notes and Other Information
- Reference counting enables safe sharing of compiled procedures across multiple call sites
- Cache invalidation uses transaction ID and tuple ID to detect pg_proc changes
- Memory context cleanup ensures no memory leaks when procedures are released
- Supports both trusted and untrusted execution contexts
- Handles complex return types including tuples, sets, and arrays
- Type conversion information is pre-computed and cached for performance