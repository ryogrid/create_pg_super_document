# ValidateJoinEstimator

## Location
[src/backend/commands/operatorcmds.c:310-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/operatorcmds.c#L310-L371)

## Overview
ValidateJoinEstimator looks up and validates a join selectivity estimator function by name, ensuring it has the correct signature and appropriate permissions for use with operators.

## Definition
```c
static Oid ValidateJoinEstimator(List *joinName)
```

## Detailed Description
This static function validates join selectivity estimator functions used by operators to estimate how selective a join condition will be during query planning. It supports both the modern 5-argument signature (introduced in PostgreSQL 8.4) and the legacy 4-argument signature for backward compatibility. The function looks up the estimator by name, verifies the correct signature and return type, and ensures the current user has EXECUTE permissions.

Join estimators help the query planner estimate what fraction of the cartesian product between two relations will satisfy a join condition involving the operator, which is essential for choosing efficient join algorithms and join orders.

The function handles signature ambiguity by preferring the 5-argument form when both exist, and reports an error if both signatures are found for the same function name.

## Parameters / Member Variables
- `joinName`: List containing the qualified name of the join estimator function to validate

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md) (multiple calls for signature lookup and validation)
  - [get_func_rettype](../g/get_func_rettype.md) (return type validation)
  - [object_aclcheck](../o/object_aclcheck.md) (permission checking)
  - [aclcheck_error](../a/aclcheck_error.md) (error reporting for permission failures)
  - [NameListToString](../N/NameListToString.md) (function name formatting for error messages)
- Called from (representative examples):
  - [DefineOperator](../D/DefineOperator.md) (during operator creation)
  - [AlterOperator](../A/AlterOperator.md) (during operator modification)

## Notes and Other Information
- The function is static and only used within operatorcmds.c
- Supports two function signatures:
  - Modern (5-arg): (internal, oid, internal, int2, internal) -> float8
  - Legacy (4-arg): (internal, oid, internal, int2) -> float8
- The 5-argument form includes SpecialJoinInfo parameter for better estimation accuracy
- Parameters represent: PlannerInfo structure, operator OID, argument list, join type, and optionally SpecialJoinInfo
- Requires EXECUTE permission on the estimator function
- Return value is the OID of the validated estimator function
- Reports ambiguity error if both 4-arg and 5-arg versions exist
- Estimator functions must return float8 values representing join selectivity