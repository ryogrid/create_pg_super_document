# print_function_arguments

## Location
[src/backend/utils/adt/ruleutils.c:3252-3399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3252-L3399)

## Overview
A comprehensive static helper function that formats and appends function arguments to a StringInfo buffer, supporting various argument modes, defaults, and special handling for table functions and ordered-set aggregates.

## Definition
```c
static int print_function_arguments(StringInfo buf, HeapTuple proctup, bool print_table_args, bool print_defaults)
```

## Detailed Description
This core utility function handles the complex formatting of function arguments for various PostgreSQL contexts. It can selectively print table arguments vs. regular arguments, include or exclude parameter defaults, and handles special cases like ordered-set aggregates and procedures. The function processes argument modes (IN, OUT, INOUT, VARIADIC, TABLE), manages argument names and types, and formats default expressions when requested. It also implements special logic for ordered-set aggregates that require 'ORDER BY' insertion and variadic argument handling.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted arguments to
- `proctup`: HeapTuple containing the function's metadata from pg_proc
- `print_table_args`: If true, prints only TABLE mode arguments; if false, prints all other argument modes
- `print_defaults`: If true, includes DEFAULT clauses for arguments that have default values

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - [get_func_arg_info](../g/get_func_arg_info.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - list_head
  - PROKIND_AGGREGATE
  - Form_pg_aggregate
  - AGGKIND_IS_ORDERED_SET
  - PROARGMODE_IN/INOUT/OUT/VARIADIC/TABLE
  - PROKIND_PROCEDURE
  - [quote_identifier](../q/quote_identifier.md)
  - [lnext](../l/lnext.md)
  - [deparse_expression](../d/deparse_expression.md)
- Called from (representative examples):
  - NameHashEntry
  - [pg_get_functiondef](pg_get_functiondef.md)
  - [pg_get_function_arguments](pg_get_function_arguments.md)
  - [pg_get_function_identity_arguments](pg_get_function_identity_arguments.md)
  - [print_function_rettype](print_function_rettype.md)

## Notes and Other Information
- Returns the number of arguments actually printed to the buffer
- Handles complex PostgreSQL-specific features like ordered-set aggregates with ORDER BY clauses
- Implements special argument mode handling for procedures to avoid SQL syntax ambiguity
- Processes argument defaults by parsing stored default expressions from pg_proc
- The function supports both table function argument formatting and regular function argument formatting
- Includes a 'nasty hack' for variadic ordered-set aggregates that requires printing the last argument twice
- Part of the core infrastructure for generating SQL DDL statements and function signatures
- The print_table_args parameter allows the same function to handle both regular arguments and table function column specifications