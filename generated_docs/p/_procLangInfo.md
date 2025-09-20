# _procLangInfo

## Location
[src/bin/pg_dump/pg_dump.h:496-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L496-L504)

## Overview
The  structure represents procedural language information in PostgreSQL's pg_dump utility, storing metadata about installed procedural languages and their associated functions.

## Definition

```c
typedef struct _procLangInfo
{
	DumpableObject dobj;
	DumpableAcl dacl;
	bool		lanpltrusted;
	Oid			lanplcallfoid;
	Oid			laninline;
	Oid			lanvalidator;
	const char *lanowner;
} ProcLangInfo;
```
## Detailed Description
The  structure captures all essential information about procedural languages registered in PostgreSQL. This includes security settings (trusted/untrusted status), associated handler functions, and ownership information. The structure is used by pg_dump to preserve procedural language definitions during database backup and restoration operations.

## Parameters / Member Variables
- : Base  containing common metadata for dump operations
- :  structure for handling access control list information
- : Boolean indicating whether the language is trusted (can be used by non-superusers)
- : OID of the call handler function for the procedural language
- : OID of the inline handler function (for anonymous code blocks)
- : OID of the validator function used to check language syntax
- : Name of the user who owns the procedural language

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- Trusted languages (lanpltrusted=true) can be used by regular database users, while untrusted languages require superuser privileges
- The call handler function (lanplcallfoid) is mandatory and handles the execution of code written in this language
- The inline handler (laninline) is optional and only needed if the language supports anonymous code blocks (DO statements)
- The validator function (lanvalidator) is optional but recommended for syntax checking during function creation
- This structure enables pg_dump to recreate procedural languages with their exact configuration during database restoration