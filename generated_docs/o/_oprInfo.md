# _oprInfo

## Location
[src/bin/pg_dump/pg_dump.h:251-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L251-L258)

## Overview
The _oprInfo structure represents operator metadata used by PostgreSQL's pg_dump utility to store information about database operators during the dump process.

## Definition

```c
typedef struct _oprInfo
{
	DumpableObject dobj;
	const char *rolname;
	char		oprkind;
	Oid			oprleft;
	Oid			oprright;
	Oid			oprcode;
} OprInfo;
```
## Detailed Description
The _oprInfo structure is used by pg_dump to manage operator information during database dumping operations. It extends the base DumpableObject structure with operator-specific metadata including the operator's owner, kind (binary, unary left, unary right), operand types, and the implementing function. This structure captures the essential information needed to recreate operators in the target database, including their signatures and implementation details.

## Parameters / Member Variables
- `dobj`: Base dumpable object structure containing common dump metadata including operator name and namespace
- `*rolname`: Name of the role (user) who owns the operator
- `oprkind`: Character indicating the operator kind ('b' for binary, 'l' for left unary, 'r' for right unary)
- `oprleft`: OID of the left operand type (InvalidOid for right unary operators)
- `oprright`: OID of the right operand type (InvalidOid for left unary operators)
- `oprcode`: OID of the function that implements this operator
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found (likely used internally by pg_dump operator-handling functions)

## Notes and Other Information
- This structure is essential for pg_dump's operator management system
- The oprkind field distinguishes between binary operators (like +, -) and unary operators (like -, NOT)
- Left and right operand types define the operator's signature and determine overload resolution
- The oprcode field links the operator to its implementing function, ensuring proper functionality restoration
- Operators in PostgreSQL can be overloaded based on their operand types, making type information crucial
- This structure supports PostgreSQL's extensible operator system where users can define custom operators
- The structure does not include additional operator properties like precedence or associativity, which are handled separately in the system catalogs