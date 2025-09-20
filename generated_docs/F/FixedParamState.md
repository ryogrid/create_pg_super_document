# FixedParamState

## Location
[src/backend/parser/parse_param.c:36-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L36-L40)

## Overview
FixedParamState is a structure used to store parameter type information for SQL queries with a fixed, pre-determined number of parameters during parsing.

## Definition

```c
typedef struct FixedParamState
{
	const Oid  *paramTypes;		/* array of parameter type OIDs */
	int			numParams;		/* number of array entries */
} FixedParamState;
```
## Detailed Description
FixedParamState is used in PostgreSQL's parser to handle parameter references in prepared statements and other queries where the parameter types and count are known in advance. This structure maintains a read-only array of parameter type OIDs and the count of parameters. It is used as part of the parameter reference hook mechanism in the parser state to validate and type-check parameter references during query parsing.

The structure is designed for scenarios where the parameter list is immutable during parsing, as opposed to variable parameter scenarios where the parameter list can grow dynamically.

## Parameters / Member Variables
- : A pointer to a constant array of Oid values representing the data types of the parameters. This array is read-only and contains the pre-determined parameter types.
- : An integer specifying the total number of parameters in the paramTypes array.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [setup_parse_fixed_parameters](../s/setup_parse_fixed_parameters.md)
  - [fixed_paramref_hook](../f/fixed_paramref_hook.md)

## Notes and Other Information
- This structure is allocated using palloc() and attached to the ParseState's p_ref_hook_state field
- Used in conjunction with the fixed_paramref_hook function to handle parameter references during parsing
- The paramTypes array is marked as const, indicating that the parameter types cannot be modified once set
- This is part of PostgreSQL's parameter handling system for prepared statements and parameterized queries
- Located in src/backend/parser/parse_param.c:36-40