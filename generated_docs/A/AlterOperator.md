# AlterOperator

## Location
src/backend/commands/operatorcmds.c: 462 - 701

## Overview
Implements the `ALTER OPERATOR` SQL command to modify specific attributes of existing operators, including restriction/join estimators and operator properties like commutator, negator, merges, and hashes.

## Definition
```c
ObjectAddress AlterOperator(AlterOperatorStmt *stmt)
```

## Detailed Description
The `AlterOperator` function processes `ALTER OPERATOR <operator> SET (option = ...)` statements to modify existing operator definitions in the PostgreSQL catalog. The function enforces strict rules about which attributes can be changed to maintain system integrity and prevent invalidation of existing query plans.

Currently supported modifications include:
- **RESTRICT and JOIN estimator functions**: Can be changed or removed at any time
- **COMMUTATOR, NEGATOR, MERGES, and HASHES attributes**: Can only be set if they were not previously defined (to prevent plan invalidation)

The function performs comprehensive validation of the requested changes, updates the `pg_operator` catalog, maintains dependency relationships, and ensures bidirectional consistency for commutator and negator relationships.

## Parameters / Member Variables
- `stmt`: Pointer to `AlterOperatorStmt` structure containing:
  - `opername`: The operator name and argument types to be modified
  - `options`: List of `DefElem` structures specifying the attributes to change

## Dependencies
- Functions called/Symbols referenced:
  - `LookupOperWithArgs`: Resolves operator name to OID
  - `SearchSysCacheCopy1`: Retrieves operator tuple from system catalog
  - `defGetQualifiedName`: Extracts qualified names from DefElem options
  - `defGetBoolean`: Extracts boolean values from DefElem options  
  - `object_ownercheck`: Verifies user ownership permissions
  - `ValidateRestrictionEstimator`: Validates restriction selectivity estimator function
  - `ValidateJoinEstimator`: Validates join selectivity estimator function
  - `ValidateOperatorReference`: Validates commutator/negator operator references
  - `OperatorValidateParams`: Performs logical consistency validation
  - `heap_modify_tuple`: Creates modified catalog tuple
  - `CatalogTupleUpdate`: Updates the catalog with changes
  - `makeOperatorDependencies`: Updates dependency relationships
  - `OperatorUpd`: Updates back-references in related operators
  - `InvokeObjectPostAlterHook`: Triggers post-alter event hooks

- Called from (representative examples):
  - `ProcessUtilitySlow`: Main utility command processing entry point

## Notes and Other Information
- **Security**: Requires ownership of the operator being modified
- **Immutable attributes**: Function, leftarg, rightarg, and procedure cannot be changed after creation
- **Plan stability**: Commutator, negator, merges, and hashes attributes can only be set once to prevent invalidation of cached query plans
- **Validation**: All changes undergo the same validation as operator creation via `OperatorValidateParams`
- **Bidirectional consistency**: When commutator or negator relationships are established, both operators are updated via `OperatorUpd`
- **Self-reference protection**: Prevents operators from being their own negator
- **Dependency management**: Automatically updates object dependencies when relationships change