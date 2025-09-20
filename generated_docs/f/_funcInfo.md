# _funcInfo

## Location
[src/bin/pg_dump/pg_dump.h:232-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L232-L241)

## Overview
The _funcInfo structure represents function metadata used by PostgreSQL's pg_dump utility to store information about database functions during the dump process.

## Definition

```c
typedef struct _funcInfo
{
	DumpableObject dobj;
	DumpableAcl dacl;
	const char *rolname;
	Oid			lang;
	int			nargs;
	Oid		   *argtypes;
	Oid			prorettype;
	bool		postponed_def;	/* function must be postponed into post-data */
} FuncInfo;
```
## Detailed Description
The _funcInfo structure is used by pg_dump to manage function information during database dumping operations. It extends the base DumpableObject and DumpableAcl structures with function-specific metadata including the function's programming language, argument types, return type, and owner information. The structure also includes a flag to indicate whether the function definition should be postponed to the post-data section of the dump, which is necessary for functions that depend on other objects that haven't been created yet.

## Parameters / Member Variables
- : Base dumpable object structure containing common dump metadata including function name and namespace
- : Access control list information for the function's permissions
- : Name of the role (user) who owns the function
- : OID of the programming language used to implement the function (e.g., SQL, PL/pgSQL, C)
- : Number of arguments the function accepts
- : Array of OIDs representing the types of the function's arguments
- : OID of the function's return type
- : Boolean flag indicating whether the function definition must be deferred to the post-data section

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - No direct references found (likely used internally by pg_dump functions)

## Notes and Other Information
- This structure is fundamental to pg_dump's function management system
- The postponed_def flag is crucial for handling complex dependency scenarios where functions reference objects that must be created first
- The argtypes array allows proper reconstruction of function signatures during restore
- Function language information is preserved to ensure proper restoration in the target database
- The structure supports PostgreSQL's polymorphic function system through proper type tracking
- Used as the base structure for aggregate functions (see _aggInfo which extends this structure)