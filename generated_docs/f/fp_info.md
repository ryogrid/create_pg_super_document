# fp_info

## Location
[src/backend/tcop/fastpath.c:49-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/fastpath.c#L49-L67)

## Overview
The  struct stores cached function metadata for PostgreSQL's fastpath function call interface, containing essential information needed to execute functions via the fastpath protocol.

## Definition

```c
struct fp_info
{
	Oid			funcid;
	FmgrInfo	flinfo;			/* function lookup info for funcid */
	Oid			namespace;		/* other stuff from pg_proc */
	Oid			rettype;
	Oid			argtypes[FUNC_MAX_ARGS];
	char		fname[NAMEDATALEN]; /* function name for logging */
};
```
## Detailed Description
The  struct is a data structure used in PostgreSQL's fastpath function call mechanism to store function metadata retrieved from the system catalog. This struct serves as a temporary container for function information needed during fastpath function execution. The fastpath interface allows clients to call PostgreSQL functions directly via the libpq protocol using PQfn(), bypassing the SQL parser for performance optimization.

Historically, this structure was designed for caching function information across transaction commands, but the caching mechanism was removed because it proved ineffective - each fastpath call executes as a separate transaction command, making the cache unusable. The current implementation fetches the information fresh for each call from the system catalogs.

The struct is populated by the  function, which performs catalog lookups to validate the function and extract its metadata from the  system catalog.

## Parameters / Member Variables
- `funcid`: The OID of the function being called; set to InvalidOid initially and only set to the correct value when the struct is fully populated
- `flinfo`: Function manager information structure containing the actual function pointer and related data needed for function execution
- `namespace`: The OID of the schema/namespace containing the function (from pg_proc.pronamespace)
- `rettype`: The OID of the function's return type (from pg_proc.prorettype)
- `argtypes[FUNC_MAX_ARGS]`: Array of OIDs representing the function's parameter types (from pg_proc.proargtypes)
- `fname[NAMEDATALEN]`: Function name string stored for logging and error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - FUNC_MAX_ARGS (maximum number of function arguments)
  - NAMEDATALEN (maximum length for PostgreSQL names)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager info structure)
  - Oid (object identifier type)

- Called from (representative examples):
  - [fetch_fp_info](fetch_fp_info.md) (populates the struct with function metadata)
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md) (uses the struct during fastpath function execution)
  - [parse_fcall_arguments](../p/parse_fcall_arguments.md) (accesses the struct to parse function arguments)

## Notes and Other Information
- The struct is designed to be zeroed out initially with MemSet() and funcid set to InvalidOid for safety
- The funcid field serves as a validity indicator - it's only set to the correct value as the last step in fetch_fp_info()
- The struct enforces the FUNC_MAX_ARGS limit on the number of function parameters
- Only functions (not procedures or aggregates) with prokind = PROKIND_FUNCTION are supported
- Set-returning functions (proretset = true) are explicitly rejected by the fastpath interface
- Function name storage is limited to NAMEDATALEN characters for compatibility with PostgreSQL's naming conventions
- This structure is allocated locally in HandleFunctionRequest() and does not persist across function calls