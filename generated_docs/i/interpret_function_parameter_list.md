# interpret_function_parameter_list

## Location
[src/backend/commands/functioncmds.c:183-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L183-L499)

## Overview
Interprets and validates the function parameter list of CREATE FUNCTION, CREATE PROCEDURE, or CREATE AGGREGATE statements, extracting type information, parameter modes, names, and default values while enforcing various constraints.

## Definition

```c
struct the proper outputs as needed */
	*parameterTypes = buildoidvector(inTypes, inCount);
```
## Detailed Description
This comprehensive function processes the parameter list for database objects (functions, procedures, aggregates) by validating parameter types, modes, names, and default values. It enforces language-specific restrictions, such as preventing SQL functions from using shell types and disallowing set arguments for all object types. The function handles variadic parameters with proper validation, ensures unique parameter names within appropriate scopes, and manages parameter ordering constraints. It also processes default expressions and validates that parameters with defaults appear at the end of the input parameter list.

## Parameters / Member Variables
- : ParseState for expression transformation and validation
- : List of FunctionParameter structs representing the parameter specification
- : OID of the function language (InvalidOid for aggregates)
- : Type of object being created (OBJECT_FUNCTION, OBJECT_PROCEDURE, or OBJECT_AGGREGATE)
- : Output oidvector containing input parameter type OIDs
- : Output list of input parameter type OIDs (optional)
- : Output array of all parameter types including OUT parameters (optional)
- : Output array of parameter modes (IN, OUT, INOUT, VARIADIC, TABLE) (optional)
- : Output array of parameter names (optional)
- : Output list of input parameter names (optional)
- : Output list of default value expressions (optional)
- : Output OID of variadic array type, or InvalidOid if none
- : Output OID of required result type based on OUT parameters

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](../L/LookupTypeName.md): Resolves type names to type information
  - [TypeNameToString](../T/TypeNameToString.md): Converts TypeName to string representation
  - [typeTypeId](../t/typeTypeId.md): Extracts OID from type tuple
  - [object_aclcheck](../o/object_aclcheck.md): Verifies type usage permissions
  - [aclcheck_error_type](../a/aclcheck_error_type.md): Reports type access permission errors
  - [get_element_type](../g/get_element_type.md): Validates variadic array types
  - [transformExpr](../t/transformExpr.md): Processes default value expressions
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md): Type coercion for default values
  - [assign_expr_collations](../a/assign_expr_collations.md): Assigns collations to expressions
  - [contain_var_clause](../c/contain_var_clause.md): Checks for table references in defaults
  - [buildoidvector](../b/buildoidvector.md): Creates oidvector for input types
  - [construct_array_builtin](../c/construct_array_builtin.md): Creates system arrays for metadata
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md): Function creation process
  - [DefineAggregate](../D/DefineAggregate.md): Aggregate creation process

## Notes and Other Information
- SQL functions and aggregates cannot accept shell types, but C functions can with warnings
- Set arguments (SETOF types) are prohibited for all object types  
- VARIADIC parameters must be the last input parameter and must be array types
- Parameter names must be unique within input or output parameter groups
- Default values are only allowed for input parameters and must appear at the end
- Procedures with OUT parameters always return RECORD type
- Functions with multiple OUT parameters return RECORD type
- The function enforces strict parameter ordering: regular inputs, then VARIADIC, then outputs
- Default expressions are validated to prevent table references, subqueries, and aggregates