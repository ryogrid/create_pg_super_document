# get_func_result_type

## Location
[src/backend/utils/fmgr/funcapi.c:410-429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L410-L429)

## Overview
Determines the datatype that a PostgreSQL function returns based solely on the function's OID, providing a simpler interface than other result type functions but with limitations in resolving complex types.

## Definition
```c
TypeFuncClass get_func_result_type(Oid functionId,
                                   Oid *resultTypeId,
                                   TupleDesc *resultTupleDesc)
```

## Detailed Description
This function provides a simplified interface for determining function result types when only the function OID is available. It serves as a lightweight wrapper around `internal_get_result_type`, passing NULL for both the expression node and ReturnSetInfo parameters.

Due to the limited information available (only function OID), this function has important limitations:
- **Cannot resolve pure-RECORD results**: Without expression context or ReturnSetInfo, it cannot determine the structure of RECORD types
- **Cannot resolve polymorphism**: Polymorphic types (ANYELEMENT, etc.) require actual argument types for resolution, which are not available with just the function OID

The function is primarily useful for cases where you need basic type information about a function and can accept these limitations, such as during catalog validation or simple type checking scenarios.

## Parameters / Member Variables
- `functionId`: The OID of the function whose result type is to be determined
- `resultTypeId`: Output parameter that receives the actual datatype OID (can be NULL if not needed)
- `resultTupleDesc`: Output parameter that receives a TupleDesc pointer for composite types or NULL for scalar results (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - TypeFuncClass (return type)
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md)

## Notes and Other Information
- Simplest interface among the result type functions, requiring only a function OID
- Cannot resolve complex type scenarios due to lack of context information
- Passes NULL for both expression and ReturnSetInfo parameters to internal_get_result_type
- Primarily used in catalog validation and scenarios where limited type information is acceptable
- Located in src/backend/utils/fmgr/funcapi.c at lines 410-429
- Part of PostgreSQL's function manager API for basic function type introspection