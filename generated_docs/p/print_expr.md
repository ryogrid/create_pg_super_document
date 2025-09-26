# print_expr

## Location
src/backend/nodes/print.c: 321 - 425

## Overview
A recursive debugging utility function that prints PostgreSQL expressions in a human-readable format, handling various expression types including variables, constants, operators, and function calls.

## Definition
```c
void print_expr(const Node *expr, const List *rtable)
```

## Detailed Description
The `print_expr` function is a comprehensive expression printing utility that recursively displays PostgreSQL expression trees in a readable format. It handles multiple expression types including Var nodes (column references), Const nodes (literal values), OpExpr nodes (operator expressions), and FuncExpr nodes (function calls). The function uses the provided range table to resolve variable names and formats the output to closely resemble SQL syntax. For complex expressions, it recursively calls itself to print sub-expressions, creating a complete textual representation of the expression tree.

## Parameters / Member Variables
- `expr`: A const pointer to the Node representing the expression to be printed (can be NULL)
- `rtable`: A const pointer to a List containing RangeTblEntry objects used to resolve variable names

## Dependencies
- Functions called/Symbols referenced:
  - IsA: PostgreSQL macro for type checking nodes
  - rt_fetch: Retrieves a range table entry by index
  - get_rte_attribute_name: Gets the name of an attribute from a range table entry
  - getTypeOutputInfo: Gets output function information for a data type
  - OidOutputFunctionCall: Calls the output function for a data type
  - get_opname: Gets the name of an operator by OID
  - get_leftop/get_rightop: Extract left and right operands from expressions
  - get_func_name: Gets the name of a function by OID
  - printf: Standard C library function for formatted output
  - pfree: Frees allocated memory
  - list_length: Gets the length of a PostgreSQL list
  - foreach/lfirst/lnext: PostgreSQL list iteration macros
  - INNER_VAR/OUTER_VAR/INDEX_VAR: Special variable number constants

- Called from (representative examples):
  - print_expr: Recursive self-calls for sub-expressions
  - print_pathkeys: Used to print expressions within path keys
  - print_tl: Used to print expressions within target lists
  - nodeDisplay: Header declaration and debugging macros

## Notes and Other Information
- Handles NULL expressions by printing "<>"
- Supports special variable types (INNER, OUTER, INDEX) with descriptive names
- Formats operator expressions in infix notation when binary, prefix when unary
- Function calls are formatted with parentheses and comma-separated arguments
- Recursively processes complex nested expressions
- Uses the range table to resolve column names to "relation.attribute" format
- Handles NULL constants specially by printing "NULL"
- Falls back to "unknown expr" for unrecognized expression types
- Memory management with pfree for allocated strings
- Located in src/backend/nodes/print.c:321-425