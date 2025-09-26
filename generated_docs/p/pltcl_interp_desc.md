# pltcl_interp_desc

## Location
[src/pl/tcl/pltcl.c:117-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L117-L122)

## Overview
A struct representing information associated with a Tcl interpreter in the PostgreSQL PL/Tcl procedural language extension, managing separate interpreters for trusted and untrusted functions.

## Definition

```c
typedef struct pltcl_interp_desc
{
	Oid			user_id;		/* Hash key (must be first!) */
	Tcl_Interp *interp;			/* The interpreter */
	Tcl_HashTable query_hash;	/* pltcl_query_desc structs */
} pltcl_interp_desc;
```
## Detailed Description
The  structure encapsulates the state and configuration of a Tcl interpreter within PostgreSQL's PL/Tcl language extension. PostgreSQL uses different security models for trusted (pltcl) and untrusted (pltclu) Tcl functions:

- **Untrusted functions (pltclu)**: Use a single shared interpreter with OID 0 as the key
- **Trusted functions (pltcl)**: Each SQL user gets a separate interpreter identified by their user OID

This separation ensures that unprivileged users cannot inject Tcl code that would execute with elevated privileges of other SQL users. The structures are stored in a PostgreSQL hash table indexed by the user_id field.

## Parameters / Member Variables
- : OID serving as the hash key for the interpreter table; must be the first field for hash table functionality. Set to 0 for the single untrusted interpreter, or to the actual user OID for trusted interpreters
- : Pointer to the actual Tcl interpreter instance that executes PL/Tcl functions
- : Hash table containing  structures that manage prepared queries and cached execution plans for this interpreter

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - Tcl_Interp (Tcl interpreter structure)
  - Tcl_HashTable (Tcl hash table structure)
- Called from (representative examples):
  - pltcl_proc_desc (referenced at line 150)
  - _PG_init (referenced at line 446)  
  - pltcl_init_interp (referenced at lines 490, 562)
  - pltcl_fetch_interp (referenced at line 566)

## Notes and Other Information
- The user_id field must be the first member to serve as the hash key in PostgreSQL's hash table implementation
- This structure is central to PL/Tcl's security model, enabling privilege separation between different SQL users
- The query_hash member allows each interpreter to maintain its own cache of prepared queries and execution plans
- Located in src/pl/tcl/pltcl.c at lines 117-122