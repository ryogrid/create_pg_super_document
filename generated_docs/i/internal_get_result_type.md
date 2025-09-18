# internal_get_result_type

## Location
[src/backend/utils/fmgr/funcapi.c:430-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L430-L550)

## Overview
The core workhorse function that implements result type determination for all PostgreSQL result type functions, handling complex scenarios like OUT parameters, polymorphic types, and RECORD resolution.

## Definition
```c
static TypeFuncClass internal_get_result_type(Oid funcid,
                                              Node *call_expr,
                                              ReturnSetInfo *rsinfo,
                                              Oid *resultTypeId,
                                              TupleDesc *resultTupleDesc)
```

## Detailed Description
This static function serves as the comprehensive implementation behind all the public result type determination functions. It performs sophisticated analysis of function return types through multiple strategies:

1. **System Catalog Lookup**: Retrieves the function's pg_proc entry to examine the declared return type
2. **OUT Parameter Handling**: Uses `build_function_result_tupdesc_t` to check for OUT parameters that define RECORD structures, resolving polymorphic OUT parameters when possible
3. **Polymorphic Type Resolution**: For scalar polymorphic results, attempts to resolve the actual type using expression type information
4. **Result Type Classification**: Uses `get_type_func_class` to determine if the result is scalar, composite, composite domain, or record
5. **Context-based RECORD Resolution**: For pure RECORD types, attempts to resolve using ReturnSetInfo context when available

The function handles the most complex scenarios in PostgreSQL's type system, including polymorphic type resolution and RECORD type determination. When complete information isn't available, it returns TYPEFUNC_RECORD and sets resultTupleDesc to NULL.

## Parameters / Member Variables
- `funcid`: The OID of the function (required parameter)
- `call_expr`: Expression node for context-based type resolution (can be NULL)
- `rsinfo`: Return set information for RECORD type resolution (can be NULL)
- `resultTypeId`: Output parameter for the result type OID (can be NULL)
- `resultTupleDesc`: Output parameter for composite type descriptor (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [build_function_result_tupdesc_t](../b/build_function_result_tupdesc_t.md)
  - [resolve_polymorphic_tupdesc](../r/resolve_polymorphic_tupdesc.md)
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md)
  - IsPolymorphicType
  - exprType
  - [get_type_func_class](../g/get_type_func_class.md)
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [get_call_result_type](../g/get_call_result_type.md)
  - [get_expr_result_type](../g/get_expr_result_type.md) (twice)
  - [get_func_result_type](../g/get_func_result_type.md)

## Notes and Other Information
- This is a static function, not exposed in the public API
- Serves as the central implementation for all result type determination logic
- Handles the most complex type resolution scenarios in PostgreSQL
- Always requires funcid parameter, but call_expr and rsinfo are optional
- Returns TYPEFUNC_RECORD when complete rowtype information cannot be determined
- Includes comprehensive error handling for invalid function OIDs and unresolvable polymorphic types
- Located in src/backend/utils/fmgr/funcapi.c at lines 430-550
- Forms the foundation of PostgreSQL's function result type introspection system