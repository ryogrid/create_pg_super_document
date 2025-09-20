# VarParamState

## Location
[src/backend/parser/parse_param.c:48-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L48-L52)

## Overview
VarParamState is a structure used to store parameter type information for SQL queries with a variable number of parameters that can be dynamically determined and expanded during parsing.

## Definition

```c
typedef struct VarParamState
{
	Oid		  **paramTypes;		/* array of parameter type OIDs */
	int		   *numParams;		/* number of array entries */
} VarParamState;
```
## Detailed Description
VarParamState is used in PostgreSQL's parser to handle parameter references in queries where the parameter types and count are not predetermined and can vary dynamically. Unlike FixedParamState, this structure uses pointers to pointers and counts, allowing the parameter array to be re-palloc'd (reallocated) to accommodate additional parameters discovered during parsing.

The structure supports parameter type resolution where initially unknown parameter types (marked as UNKNOWNOID) can be determined and updated as the parser encounters more context. A zero array entry indicates that a parameter number hasn't been encountered yet, while UNKNOWNOID indicates the parameter has been used but its type is still being determined.

## Parameters / Member Variables
- : A pointer to a pointer to an array of Oid values. This double indirection allows the array to be reallocated and expanded as needed during parsing.
- : A pointer to an integer that tracks the current number of parameters in the paramTypes array. The pointed-to value can be updated as parameters are added.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - UNKNOWNOID (special OID value for unknown types)
- Called from (representative examples):
  - [setup_parse_variable_parameters](../s/setup_parse_variable_parameters.md)
  - [variable_paramref_hook](../v/variable_paramref_hook.md)
  - [variable_coerce_param_hook](../v/variable_coerce_param_hook.md)
  - [check_variable_parameters](../c/check_variable_parameters.md)
  - [check_parameter_resolution_walker](../c/check_parameter_resolution_walker.md)

## Notes and Other Information
- This structure is allocated using palloc() and attached to the ParseState's p_ref_hook_state field
- Used in conjunction with variable_paramref_hook and variable_coerce_param_hook functions
- The double pointer design allows the underlying parameter array to be reallocated and expanded during parsing
- Supports dynamic parameter type resolution where unknown types can be inferred from context
- Zero entries in the paramTypes array indicate unused parameter slots
- UNKNOWNOID entries indicate parameters that have been referenced but whose types are still being determined
- Part of PostgreSQL's flexible parameter handling system for dynamic SQL and variable parameter scenarios
- Located in src/backend/parser/parse_param.c:48-52