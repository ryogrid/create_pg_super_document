# CreateFunctionStmt

## Location
[src/include/nodes/parsenodes.h:3427-3437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3427-L3437)

## Overview
CreateFunctionStmt represents a parsed CREATE FUNCTION or CREATE PROCEDURE statement, containing all information needed to define a user-defined function or procedure in PostgreSQL.

## Definition

```c
typedef struct CreateFunctionStmt
{
	NodeTag		type;
	bool		is_procedure;	/* it's really CREATE PROCEDURE */
	bool		replace;		/* T => replace if already exists */
	List	   *funcname;		/* qualified name of function to create */
	List	   *parameters;		/* a list of FunctionParameter */
	TypeName   *returnType;		/* the return type */
	List	   *options;		/* a list of DefElem */
	Node	   *sql_body;
} CreateFunctionStmt;
```
## Detailed Description
CreateFunctionStmt is a parse tree node that represents both CREATE FUNCTION and CREATE PROCEDURE SQL statements. It encapsulates all the parsed information needed to create a user-defined function or procedure, including the function signature, return type, parameters, and various options like language, volatility, security definer settings, and the function body.

The structure handles the dual nature of functions and procedures in PostgreSQL, where procedures are essentially functions that don't return values. The sql_body field can contain the function implementation for SQL-language functions, while other languages use the options list to specify the function implementation.

## Parameters / Member Variables
- `type`: Standard NodeTag for PostgreSQL parse tree nodes
- `is_procedure`: Boolean flag indicating this is CREATE PROCEDURE rather than CREATE FUNCTION
- `replace`: Boolean flag for CREATE OR REPLACE semantics - allows overwriting existing functions
- `*funcname`: Qualified name of the function as a list of strings (schema.function_name)
- `*parameters`: List of FunctionParameter nodes defining the function's parameter list
- `*returnType`: TypeName node specifying the return type (NULL for procedures)
- `*options`: List of DefElem nodes containing function options (LANGUAGE, VOLATILITY, SECURITY, etc.)
- `*sql_body`: Parse tree containing the function body for SQL-language functions
## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](../T/TypeName.md) (for return type specification)
  - [FunctionParameter](../F/FunctionParameter.md) (for parameter definitions)
  - [DefElem](../D/DefElem.md) (for function options)
  - NodeTag (parse tree infrastructure)
  - [List](../L/List.md) (PostgreSQL list structure)

- Called from (representative examples):
  - [CreateFunction](CreateFunction.md) (main function creation handler)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command dispatcher)
  - [CreateCommandTag](CreateCommandTag.md) (command tagging for logging)

## Notes and Other Information
- Handles both CREATE FUNCTION and CREATE PROCEDURE statements through is_procedure flag
- The replace flag implements CREATE OR REPLACE functionality
- Options list contains language specification, volatility settings, security context, cost estimates, etc.
- SQL-language functions store their body in sql_body field as a parse tree
- Other language functions specify their implementation through the options list
- Return type is NULL for procedures since they don't return values
- Part of PostgreSQL's extensibility framework for user-defined functions
- Used extensively in stored procedure and function definition processing