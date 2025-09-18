# interpret_func_support

## Location
src/backend/commands/functioncmds.c: 670 - 713

## Overview
Validates and resolves a support function specification for PostgreSQL functions, ensuring the support function meets required signature and privilege constraints.

## Definition
```c
static Oid interpret_func_support(DefElem *defel)
```

## Detailed Description
This static function processes a support function specification from a DefElem and returns the OID of the validated support function. Support functions are special functions that can provide additional optimization and execution support for user-defined functions. The function enforces strict requirements: support functions must take exactly one INTERNAL argument and return INTERNAL type.

The function performs comprehensive validation including existence checks, signature validation, and privilege verification. Only superusers are allowed to specify support functions due to their privileged nature and potential security implications.

## Parameters / Member Variables
- `defel`: DefElem pointer containing the qualified name specification of the support function to validate

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type for definition elements)
  - [defGetQualifiedName](../d/defGetQualifiedName.md) (extracts qualified name from DefElem)
  - [LookupFuncName](../L/LookupFuncName.md) (looks up function by name and signature)
  - [func_signature_string](../f/func_signature_string.md) (creates function signature string for error messages)
  - [get_func_rettype](../g/get_func_rettype.md) (gets function return type)
  - [NameListToString](../N/NameListToString.md) (converts name list to string)
  - superuser (checks if current user is superuser)
  - INTERNALOID (internal type OID constant)
- Called from (representative examples):
  - [compute_function_attributes](../c/compute_function_attributes.md) (src/backend/commands/functioncmds.c:835)
  - [AlterFunction](../A/AlterFunction.md) (src/backend/commands/functioncmds.c:1444)

## Notes and Other Information
- Support functions must have signature: `function_name(internal) RETURNS internal`
- Only superusers can specify support functions for security reasons
- The function includes a comment noting that ACL checks might be added in the future
- Generates detailed error messages for function not found and invalid signature cases
- Part of PostgreSQL's extensible function system allowing custom optimization strategies