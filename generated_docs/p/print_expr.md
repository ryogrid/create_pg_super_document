# print_expr

## Location
[src/backend/nodes/print.c:321-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/print.c#L321-L425)

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
  - [get_rte_attribute_name](../g/get_rte_attribute_name.md): Gets the name of an attribute from a range table entry
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md): Gets output function information for a data type
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md): Calls the output function for a data type
  - [get_opname](../g/get_opname.md): Gets the name of an operator by OID
  - [get_leftop](../g/get_leftop.md)/get_rightop: Extract left and right operands from expressions
  - [get_func_name](../g/get_func_name.md): Gets the name of a function by OID
  - printf: Standard C library function for formatted output
  - [pfree](pfree.md): Frees allocated memory
  - [list_length](../l/list_length.md): Gets the length of a PostgreSQL list
  - foreach/lfirst/lnext: PostgreSQL list iteration macros
  - INNER_VAR/OUTER_VAR/INDEX_VAR: Special variable number constants

- Called from (representative examples):
  - [print_expr](print_expr.md): Recursive self-calls for sub-expressions
  - [print_pathkeys](print_pathkeys.md): Used to print expressions within path keys
  - [print_tl](print_tl.md): Used to print expressions within target lists
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