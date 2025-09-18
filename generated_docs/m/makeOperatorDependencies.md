# makeOperatorDependencies

## Location
src/backend/catalog/pg_operator.c: 853 - 945

## Overview
Establishes and manages dependency relationships for operators by recording dependencies on all related database objects including types, functions, namespaces, and owners.

## Definition
```c
ObjectAddress makeOperatorDependencies(HeapTuple tuple,
                                       bool makeExtensionDep,
                                       bool isUpdate)
```

## Detailed Description
This function is responsible for creating comprehensive dependency records for operators in PostgreSQL's dependency management system. It ensures that all objects referenced by an operator are properly tracked, enabling correct cascade behavior during DROP operations and preventing inconsistent catalog states.

The function systematically records dependencies on:
- **Namespace**: The schema containing the operator
- **Argument Types**: Left and right operand types (for unary/binary operators)  
- **Result Type**: The return type of the operator
- **Implementation Function**: The function that actually performs the operation
- **Selectivity Functions**: Optional functions for query optimization (restriction and join selectivity)
- **Owner**: The user who owns the operator
- **Extension**: If created within an extension context

Notably, it does NOT create dependencies on commutator and negator operators, as these relationships require special handling during deletion (clearing links rather than cascade deletion).

## Parameters / Member Variables
- `tuple`: HeapTuple containing the operator's catalog row data
- `makeExtensionDep`: Whether to record extension membership dependency (true for new operators, false for ALTER OPERATOR)
- `isUpdate`: Whether this is updating an existing operator (triggers cleanup of old dependencies)

## Dependencies
- Functions called/Symbols referenced:
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md) (clears existing dependencies during updates)  
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md) (clears shared dependencies)
  - [new_object_addresses](../n/new_object_addresses.md)/free_object_addresses (manages dependency collection)
  - ObjectAddressSet (creates object address structures)
  - [add_exact_object_address](../a/add_exact_object_address.md) (adds dependency to collection)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md) (records normal dependencies)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md) (records ownership dependency)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md) (records extension membership)
- Called from (representative examples):
  - [OperatorShellMake](../O/OperatorShellMake.md) (at src/backend/catalog/pg_operator.c:265)
  - [OperatorCreate](../O/OperatorCreate.md) (at src/backend/catalog/pg_operator.c:517)
  - [AlterOperator](../A/AlterOperator.md) (at src/backend/commands/operatorcmds.c:691)

## Notes and Other Information
- Uses OidIsValid checks throughout to handle shell operators that may have incomplete information
- The function explicitly avoids creating dependencies on commutator/negator operators to prevent inappropriate cascade deletion
- Handles both creation and update scenarios, with updates requiring cleanup of existing dependency records
- Extension dependencies are only recorded when explicitly requested to maintain proper extension membership
- Returns the ObjectAddress of the operator for use in higher-level dependency tracking