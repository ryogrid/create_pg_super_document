# ValidateRestrictionEstimator

## Location
[src/backend/commands/operatorcmds.c:275-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/operatorcmds.c#L275-L309)

## Overview
ValidateRestrictionEstimator looks up and validates a restriction selectivity estimator function by name, ensuring it has the correct signature and appropriate permissions for use with operators.

## Definition
```c
static Oid ValidateRestrictionEstimator(List *restrictionName)
```

## Detailed Description
This static function validates restriction selectivity estimator functions used by operators to estimate how selective a restriction clause will be during query planning. It looks up the function by name and verifies that it conforms to the required signature: it must take four parameters (PlannerInfo, operator OID, args list, varRelid) and return a float8 value representing the selectivity estimate. The function also ensures the current user has EXECUTE permissions on the estimator function.

Restriction estimators are used by the query planner to estimate what fraction of rows will satisfy a WHERE clause condition involving the operator, which is crucial for generating efficient query execution plans.

## Parameters / Member Variables
- `restrictionName`: List containing the qualified name of the restriction estimator function to validate

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md) (function lookup with signature verification)
  - [get_func_rettype](../g/get_func_rettype.md) (return type validation)
  - [object_aclcheck](../o/object_aclcheck.md) (permission checking)
  - [aclcheck_error](../a/aclcheck_error.md) (error reporting for permission failures)
  - [NameListToString](../N/NameListToString.md) (function name formatting for error messages)
- Called from (representative examples):
  - [DefineOperator](../D/DefineOperator.md) (during operator creation)
  - [AlterOperator](../A/AlterOperator.md) (during operator modification)

## Notes and Other Information
- The function is static and only used within operatorcmds.c
- Required function signature: (internal, oid, internal, int4) -> float8
- The four parameters represent: PlannerInfo structure, operator OID, argument list, and variable relation ID
- Requires EXECUTE permission on the estimator function
- Return value is the OID of the validated estimator function
- Estimator functions must return float8 (double precision) values between 0.0 and 1.0 representing selectivity

## Simplified Source

```c
static Oid
ValidateRestrictionEstimator(List *restrictionName)
{
    Oid typeId[4];
    Oid restrictionOid;
    AclResult aclresult;

    // Define expected signature: (internal, oid, internal, int4) -> float8
    typeId[0] = INTERNALOID;    // PlannerInfo
    typeId[1] = OIDOID;         // operator OID
    typeId[2] = INTERNALOID;    // args list
    typeId[3] = INT4OID;        // varRelid

    // Look up function with exact signature
    restrictionOid = LookupFuncName(restrictionName, 4, typeId, false);

    // Verify function returns float8
    if (get_func_rettype(restrictionOid) != FLOAT8OID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("restriction estimator function %s must return type %s",
                        NameListToString(restrictionName), "float8")));

    // Check EXECUTE permission
    aclresult = object_aclcheck(ProcedureRelationId, restrictionOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString(restrictionName));

    return restrictionOid;
}
```