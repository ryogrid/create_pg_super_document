# setup_parse_fixed_parameters

## Location
[src/backend/parser/parse_param.c:67-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L67-L82)

## Overview
Configures a ParseState structure to handle references to fixed parameters during query parsing by setting up the necessary hook and state information.

## Definition

```c
void
setup_parse_fixed_parameters(ParseState *pstate,
							 const Oid *paramTypes, int numParams)
```
## Detailed Description
This function initializes the parameter reference handling mechanism for queries that contain fixed parameters (parameters with known types and count). It allocates and configures a FixedParamState structure that stores the parameter type information, then installs the fixed_paramref_hook as the parameter reference callback in the ParseState. This setup allows the parser to properly validate and handle parameter references (, , etc.) during query analysis when the parameter types are known in advance.

## Parameters / Member Variables
- `pstate`: ParseState structure being configured for parameter handling
- `paramTypes`: Array of Oid values representing the data types of the fixed parameters
- `numParams`: Number of parameters in the paramTypes array

## Dependencies
- Functions called/Symbols referenced:
  - [FixedParamState](../F/FixedParamState.md) (structure type)
  - [fixed_paramref_hook](../f/fixed_paramref_hook.md) (callback function)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [parse_analyze_fixedparams](../p/parse_analyze_fixedparams.md)

## Notes and Other Information
- This function is part of PostgreSQL's parameter handling infrastructure for prepared statements
- The allocated FixedParamState is stored in pstate->p_ref_hook_state for use by the parameter reference hook
- No coercion hook is needed for fixed parameters since their types are predetermined
- Memory allocation uses palloc which is automatically freed when the current memory context is destroyed