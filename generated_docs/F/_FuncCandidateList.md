# _FuncCandidateList

## Location
[src/include/catalog/namespace.h:29-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/namespace.h#L29-L39)

## Overview
A structure that holds a list of possible functions or operators found by namespace lookup, used during function resolution in PostgreSQL's parser.

## Definition

```c
typedef struct _FuncCandidateList
{
	struct _FuncCandidateList *next;
	int			pathpos;		/* for internal use of namespace lookup */
	Oid			oid;			/* the function or operator's OID */
	int			nominalnargs;	/* either pronargs or length(proallargtypes) */
	int			nargs;			/* number of arg types returned */
	int			nvargs;			/* number of args to become variadic array */
	int			ndargs;			/* number of defaulted args */
	int		   *argnumbers;		/* args' positional indexes, if named call */
	Oid			args[FLEXIBLE_ARRAY_MEMBER];	/* arg types */
}		   *FuncCandidateList;
```
## Detailed Description
This structure represents a linked list of function or operator candidates discovered during namespace lookup. Each candidate is identified by OID and argument types, but the list must be further pruned by type resolution rules implemented in the parser. The structure supports various PostgreSQL function features including variadic functions, default arguments, and named parameter calls.

The structure is primarily used by  to return a list of potential function matches that need further type resolution. It handles complex scenarios like function overloading, namespace precedence, variadic argument expansion, and default parameter insertion.

## Parameters / Member Variables
- `*next`: Pointer to the next candidate in the linked list
- `pathpos`: Internal field tracking the position in the namespace search path for conflict resolution
- `oid`: The Object Identifier of the function or operator in the system catalog
- `nominalnargs`: The nominal number of arguments (either  or length of )
- `nargs`: The actual number of argument types returned in the  array
- `nvargs`: Number of arguments that become part of a variadic array (0 if not variadic)
- `ndargs`: Number of arguments with default values that were inserted
- `*argnumbers`: Array of positional indexes for named parameter calls (NULL for positional calls)
- `args[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing the Oid types of all arguments
## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array member)
- Called from (representative examples):
  - [FuncnameGetCandidates](FuncnameGetCandidates.md) (primary creator and user)
  - SPACE_PER_OP (for memory allocation calculations)

## Notes and Other Information
- The structure uses a flexible array member for , allowing variable-length argument type arrays
- When  is set to  (0), it indicates an ambiguous match representing multiple conflicting candidates
- The  array is only populated for named parameter calls and maps logical argument positions to catalog positions
- Memory for the structure is allocated using  with size calculated based on the number of arguments
- The list maintains namespace search path precedence, with earlier namespaces masking identical entries in later namespaces
- Supports PostgreSQL's advanced function features: variadic functions, default parameters, and OUT parameters