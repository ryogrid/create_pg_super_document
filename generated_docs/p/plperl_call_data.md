# plperl_call_data

## Location
[src/pl/plperl/plperl.c:171-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L171-L181)

## Overview
The plperl_call_data structure manages the runtime context and state information for the duration of a single PL/Perl function call, including support for set-returning functions.

## Definition

```c
typedef struct plperl_call_data
{
	plperl_proc_desc *prodesc;
	FunctionCallInfo fcinfo;
	/* remaining fields are used only in a function returning set: */
	Tuplestorestate *tuple_store;
	TupleDesc	ret_tdesc;
	Oid			cdomain_oid;	/* 0 unless returning domain-over-composite */
	void	   *cdomain_info;
	MemoryContext tmp_cxt;
} plperl_call_data;
```
## Detailed Description
This structure encapsulates the runtime execution context for a single invocation of a PL/Perl function. It bridges the gap between PostgreSQL's function call infrastructure and the cached procedure information, providing a temporary workspace for function execution.

The structure serves dual purposes: basic function execution (using prodesc and fcinfo) and set-returning function support (using the remaining fields). For set-returning functions, it manages the tuple store that accumulates results across multiple return_next calls, maintains type descriptors for result formatting, and handles domain constraints over composite types.

The tmp_cxt memory context provides a scratch space that can be reset between function calls or used for temporary allocations during execution, ensuring clean memory management without affecting the longer-lived procedure cache.

## Parameters / Member Variables
- `*prodesc`: Pointer to the cached plperl_proc_desc containing the compiled function information and metadata
- `fcinfo`: PostgreSQL's FunctionCallInfo structure containing call arguments, context, and result information
- `*tuple_store`: Tuplestore for accumulating results from set-returning functions (NULL for regular functions)
- `ret_tdesc`: Tuple descriptor defining the structure of returned tuples for set-returning functions
- `cdomain_oid`: OID of domain type when returning domain-over-composite types (0 for regular composite returns)
- `*cdomain_info`: Cached domain constraint information for validation of domain-over-composite returns
- `tmp_cxt`: Temporary memory context for scratch allocations during function execution
## Dependencies
- Functions called/Symbols referenced:
  - [plperl_proc_desc](plperl_proc_desc.md) (procedure descriptor reference)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (PostgreSQL function call info)
  - [Tuplestorestate](../T/Tuplestorestate.md) (set-returning function support)
- Called from (representative examples):
  - [plperl_call_handler](plperl_call_handler.md) (main function call entry point)
  - [plperl_inline_handler](plperl_inline_handler.md) (inline code execution)

## Notes and Other Information
- Lifetime is limited to a single function call execution
- Set-returning function fields are only populated when needed, saving memory for regular functions
- Domain-over-composite support enables proper constraint validation for complex return types
- Temporary memory context enables efficient cleanup and prevents memory leaks
- Bridges cached procedure information with runtime execution state
- Critical for maintaining execution context across multiple return_next calls in SRFs
- Memory management strategy separates long-term cache from short-term execution context