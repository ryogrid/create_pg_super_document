# setup_parse_variable_parameters

## Location
src/backend/parser/parse_param.c: 83 - 98

## Overview
Configures a ParseState structure to handle references to variable parameters during query parsing by setting up the necessary hooks and state information for dynamically typed parameters.

## Definition
```c
void setup_parse_variable_parameters(ParseState *pstate, Oid **paramTypes, int *numParams)
```

## Detailed Description
This function initializes the parameter reference handling mechanism for queries that contain variable parameters (parameters whose types and count are not predetermined). It allocates and configures a VarParamState structure that stores pointers to the parameter type array and count, then installs both the variable_paramref_hook for parameter reference handling and variable_coerce_param_hook for parameter type coercion. This setup allows the parser to dynamically determine parameter types and handle type coercion during query analysis when parameter information is not known in advance.

## Parameters / Member Variables
- `pstate`: ParseState structure being configured for parameter handling
- `paramTypes`: Pointer to array pointer that will store the dynamically determined parameter types
- `numParams`: Pointer to integer that will store the count of discovered parameters

## Dependencies
- Functions called/Symbols referenced:
  - [VarParamState](../V/VarParamState.md) (structure type)
  - [variable_paramref_hook](../v/variable_paramref_hook.md) (callback function)
  - [variable_coerce_param_hook](../v/variable_coerce_param_hook.md) (callback function)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [parse_analyze_varparams](../p/parse_analyze_varparams.md)
  - [transformExplainStmt](../t/transformExplainStmt.md)

## Notes and Other Information
- This function is used for ad-hoc queries where parameter types must be inferred from context
- Unlike fixed parameters, variable parameters require both reference and coercion hooks
- The paramTypes and numParams are double pointers, allowing the hooks to modify the arrays dynamically
- Used primarily for EXPLAIN statements and other cases where parameter types are determined during parsing
- Memory allocation uses palloc which is automatically freed when the current memory context is destroyed