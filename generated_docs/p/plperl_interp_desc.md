# plperl_interp_desc

## Location
[src/pl/plperl/plperl.c:83-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L83-L88)

## Overview
The plperl_interp_desc structure represents information associated with a Perl interpreter in PostgreSQL's PL/Perl procedural language implementation. It manages interpreter instances for both trusted (plperl) and untrusted (plperlu) Perl functions.

## Definition

```c
typedef struct plperl_interp_desc
{
	Oid			user_id;		/* Hash key (must be first!) */
	PerlInterpreter *interp;	/* The interpreter */
	HTAB	   *query_hash;		/* plperl_query_entry structs */
} plperl_interp_desc;
```
## Detailed Description
This structure encapsulates the state and resources associated with a Perl interpreter instance. PostgreSQL uses different interpreter management strategies for trusted and untrusted Perl functions:

- **Untrusted functions (plperlu)**: Use a single shared interpreter with user_id 0
- **Trusted functions (plperl)**: Each effective SQL user gets a separate interpreter to ensure privilege isolation

The interpreters are stored in a hash table indexed by userid OID and are kept for the entire process lifetime. The system employs a "held interpreter" strategy that allows preloading Perl code at postmaster startup via plperl.on_init, which can then be utilized by backends for improved performance.

## Parameters / Member Variables
- : OID serving as the hash key for the interpreter lookup table (OID 0 for untrusted interpreter, actual user OID for trusted interpreters)
- : Pointer to the actual PerlInterpreter instance that executes Perl code
- : Hash table containing plperl_query_entry structures for managing prepared queries within this interpreter context

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (PostgreSQL hash table type)
- Called from (representative examples):
  - [plperl_proc_desc](plperl_proc_desc.md) (references this structure)
  - [_PG_init](../P/_PG_init.md) (initialization)
  - [select_perl_context](../s/select_perl_context.md) (interpreter selection)
  - [activate_interpreter](../a/activate_interpreter.md) (interpreter activation)
  - [compile_plperl_function](../c/compile_plperl_function.md) (function compilation)

## Notes and Other Information
- The user_id field must be first in the structure as it serves as the hash key
- Multiple interpreters are only supported if the Perl build includes multiplicity support
- The interpreter lifecycle management ensures security isolation between different SQL users
- The held interpreter mechanism optimizes startup performance by allowing code preloading
- Once created, interpreters persist for the entire backend process lifetime