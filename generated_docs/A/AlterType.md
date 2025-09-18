# AlterType

## Location
src/backend/commands/typecmds.c: 4312 - 4562

## Overview
The main entry point for executing ALTER TYPE SET commands that modify various properties of PostgreSQL base types, with strict validation and security controls.

## Definition


## Detailed Description
AlterType processes ALTER TYPE SET commands that can modify specific properties of base types including storage strategy, I/O functions (receive, send, typmod_in, typmod_out), analysis function, and subscript function. The function enforces strict limitations, allowing changes only to base types (not composite types, domains, or arrays) and requiring superuser privileges for I/O function modifications. It validates all requested changes, builds a parameters structure, and delegates the actual recursive modification to AlterTypeRecurse.

## Parameters / Member Variables
- : AlterTypeStmt structure containing the type name and list of property modifications to apply

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - [typenameType](../t/typenameType.md)
  - [typeTypeId](../t/typeTypeId.md)
  - [defGetString](../d/defGetString.md)
  - [defGetQualifiedName](../d/defGetQualifiedName.md)
  - [findTypeReceiveFunction](../f/findTypeReceiveFunction.md)
  - [findTypeSendFunction](../f/findTypeSendFunction.md)
  - [findTypeTypmodinFunction](../f/findTypeTypmodinFunction.md)
  - [findTypeTypmodoutFunction](../f/findTypeTypmodoutFunction.md)
  - [findTypeAnalyzeFunction](../f/findTypeAnalyzeFunction.md)
  - [findTypeSubscriptingFunction](../f/findTypeSubscriptingFunction.md)
  - superuser
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - IsTrueArrayType
  - [AlterTypeRecurse](AlterTypeRecurse.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Only allows modification of base types, rejecting composite types, domains, and array types
- Requires superuser privileges for changing I/O functions due to security implications
- Validates storage changes, preventing transitions from non-PLAIN to PLAIN storage
- Explicitly rejects modification of immutable type properties like input/output functions, internal length, and alignment
- Uses AlterTypeRecurseParams structure to pass modification parameters to the recursive function
- Returns ObjectAddress of the modified type for dependency tracking and further processing
- Enforces that fixed-size types (typlen != -1) can only use PLAIN storage