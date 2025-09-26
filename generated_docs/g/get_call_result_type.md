# get_call_result_type

## Location
[src/backend/utils/fmgr/funcapi.c:276-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L276-L298)

## Overview
Determines the datatype that a PostgreSQL function is supposed to return based on its function call information record, handling complex cases like RECORD resolution and polymorphic type resolution.

## Definition

```c
TypeFuncClass
get_call_result_type(FunctionCallInfo fcinfo,
					 Oid *resultTypeId,
					 TupleDesc *resultTupleDesc)
```
## Detailed Description
This function analyzes a function's call information to determine what type of data it should return. It serves as a high-level wrapper around , extracting the necessary information from the FunctionCallInfo structure. The function handles two particularly challenging scenarios:

1. **RECORD Type Resolution**: When functions return RECORD types, it attempts to resolve the actual row type from either the function's OUT parameter list or from a ReturnSetInfo context node. If resolution fails due to insufficient information, it returns TYPEFUNC_RECORD.

2. **Polymorphic Type Resolution**: It resolves polymorphic pseudotypes (ANYELEMENT, etc.) to their concrete types, ensuring that neither scalar results nor rowtype components contain unresolved polymorphic types.

The function is designed to be called sparingly in set-returning functions due to its computational cost, ideally only on the first invocation.

## Parameters / Member Variables
- : Function call information record containing the function OID, expression context, and result information
- : Output parameter that receives the actual datatype OID for scalar result types (can be NULL if not needed)
- : Output parameter that receives a TupleDesc pointer for composite types or NULL for scalar results (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (structure)
  - [ReturnSetInfo](../R/ReturnSetInfo.md) (structure)
  - TypeFuncClass (return type)
- Called from (representative examples):
  - [pg_last_committed_xact](../p/pg_last_committed_xact.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [plperl_return_next_internal](../p/plperl_return_next_internal.md)
  - [PLy_exec_function](../P/PLy_exec_function.md)

## Notes and Other Information
- This function is relatively expensive computationally and should be used judiciously in performance-critical code paths
- It never returns polymorphic pseudotypes, always resolving them to concrete types
- The function handles both scalar and composite return types through its output parameters
- Located in src/backend/utils/fmgr/funcapi.c at lines 276-298
- Part of PostgreSQL's function manager API for type introspection