# get_expr_result_type

## Location
[src/backend/utils/fmgr/funcapi.c:299-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L299-L409)

## Overview
Determines the datatype that a PostgreSQL expression is supposed to return by analyzing the expression node tree, handling various expression types including function calls, operators, row expressions, and constants.

## Definition
```c
TypeFuncClass get_expr_result_type(Node *expr,
                                   Oid *resultTypeId,
                                   TupleDesc *resultTupleDesc)
```

## Detailed Description
This function analyzes different types of expression nodes to determine their result types. It serves as a comprehensive expression type analyzer that can handle multiple expression node types:

1. **FuncExpr**: Function call expressions - delegates to internal_get_result_type using the function ID
2. **OpExpr**: Operator expressions - resolves the operator to its underlying function and delegates to internal_get_result_type
3. **RowExpr**: Row constructor expressions with RECORD type - directly constructs a TupleDesc from the row's column information
4. **Const**: Constant expressions with RECORD type - extracts type information from the constant's datum header
5. **Generic expressions**: Falls back to using exprType() and get_type_func_class() for type resolution

The function includes special handling for RECORD types in row expressions and constants, where it can resolve the actual composite structure. For RTE (Range Table Entry) expressions with column definition lists, it warns that the function should conclude RECORD type with columns defined by the coldeflist.

## Parameters / Member Variables
- `expr`: Expression node tree to analyze for type information
- `resultTypeId`: Output parameter that receives the actual datatype OID (can be NULL if not needed)
- `resultTupleDesc`: Output parameter that receives a TupleDesc pointer for composite types or NULL for scalar results (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - [get_opcode](get_opcode.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - DatumGetHeapTupleHeader
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)
  - [get_type_func_class](get_type_func_class.md)
- Called from (representative examples):
  - [init_sexpr](../i/init_sexpr.md)
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md)
  - [pull_up_constant_function](../p/pull_up_constant_function.md)
  - [inline_function](../i/inline_function.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - [get_expr_result_tupdesc](get_expr_result_tupdesc.md)

## Notes and Other Information
- Provides comprehensive expression type analysis covering the major PostgreSQL expression node types
- Includes special warning about RTE expressions with coldeflist - the correct result should be RECORD with columns defined by coldeflist fields
- Can directly resolve RECORD types in RowExpr by constructing TupleDesc from column information
- Handles RECORD-type constants that appear in EXPLAIN queries with SEARCH/CYCLE clauses by extracting typmod information
- Located in src/backend/utils/fmgr/funcapi.c at lines 299-409
- Part of PostgreSQL's function manager API for expression type introspection