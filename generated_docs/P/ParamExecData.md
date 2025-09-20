# ParamExecData

## Location
[src/include/nodes/params.h:146-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/params.h#L146-L151)

## Overview
ParamExecData is a struct used for executor internal parameters that represent values being passed into or out of sub-queries, supporting lazy evaluation through optional execution plan references.

## Definition

```c
typedef struct ParamExecData
{
	void	   *execPlan;		/* should be "SubPlanState *" */
	Datum		value;
	bool		isnull;
} ParamExecData;
```
## Detailed Description
ParamExecData entries are used for executor internal parameters (PARAM_EXEC parameters) that facilitate communication between different levels of query execution, particularly for sub-queries. The paramid of a PARAM_EXEC Param serves as a zero-based index into an array of ParamExecData records, which is referenced through es_param_exec_vals or ecxt_param_exec_vals in execution contexts.

The structure supports lazy evaluation of InitPlans and sub-plans: when execPlan is not NULL, it points to a SubPlanState node that will be executed only when the parameter value is actually needed. This design optimizes query execution by avoiding unnecessary sub-plan execution.

When execPlan is NULL, the value and isnull fields contain the actual parameter data that is assumed to be valid when accessed.

## Parameters / Member Variables
- : Pointer to SubPlanState node for lazy evaluation (cast as void* for header independence); NULL if value is pre-computed
- : The actual parameter value as a PostgreSQL Datum
- : Boolean flag indicating whether the parameter value is SQL NULL

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - [SubPlanState](../S/SubPlanState.md) (implicitly referenced through execPlan)

- Called from (representative examples):
  - [ExecEvalParamExec](../E/ExecEvalParamExec.md) (parameter evaluation)
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md) (executor initialization)
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md) (sub-plan scanning)
  - [ExecSetParamPlan](../E/ExecSetParamPlan.md) (parameter plan setting)
  - SerializeParamExecParams (parallel execution serialization)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md) (sub-plan initialization)

## Notes and Other Information
- Critical for inter-query parameter passing in complex queries with sub-selects
- Enables lazy evaluation optimization for InitPlans and sub-plans
- Used extensively in parallel query execution for parameter serialization/deserialization
- The execPlan field allows for deferred execution until parameter values are actually needed
- Essential for proper handling of correlated sub-queries and CTEs (Common Table Expressions)
- Values are stored in executor state structures (EState, ExprContext) for efficient access during query execution