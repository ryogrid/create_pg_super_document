# get_call_result_type

## Location
src/backend/utils/fmgr/funcapi.c: 276 - 298

## Overview
Determines the datatype that a PostgreSQL function is supposed to return based on its function call information record, handling complex cases like RECORD resolution and polymorphic type resolution.

## Definition


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
  - internal_get_result_type
  - FunctionCallInfo (structure)
  - ReturnSetInfo (structure)
  - TypeFuncClass (return type)
- Called from (representative examples):
  - pg_last_committed_xact
  - init_sql_fcache
  - InitMaterializedSRF
  - plperl_return_next_internal
  - PLy_exec_function

## Notes and Other Information
- This function is relatively expensive computationally and should be used judiciously in performance-critical code paths
- It never returns polymorphic pseudotypes, always resolving them to concrete types
- The function handles both scalar and composite return types through its output parameters
- Located in src/backend/utils/fmgr/funcapi.c at lines 276-298
- Part of PostgreSQL's function manager API for type introspection