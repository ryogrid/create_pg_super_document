# CreateFunctionStmt

## Location
src/include/nodes/parsenodes.h: 3427 - 3437

## Overview
CreateFunctionStmt represents a parsed CREATE FUNCTION or CREATE PROCEDURE statement, containing all information needed to define a user-defined function or procedure in PostgreSQL.

## Definition


## Detailed Description
CreateFunctionStmt is a parse tree node that represents both CREATE FUNCTION and CREATE PROCEDURE SQL statements. It encapsulates all the parsed information needed to create a user-defined function or procedure, including the function signature, return type, parameters, and various options like language, volatility, security definer settings, and the function body.

The structure handles the dual nature of functions and procedures in PostgreSQL, where procedures are essentially functions that don't return values. The sql_body field can contain the function implementation for SQL-language functions, while other languages use the options list to specify the function implementation.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL parse tree nodes
- : Boolean flag indicating this is CREATE PROCEDURE rather than CREATE FUNCTION
- : Boolean flag for CREATE OR REPLACE semantics - allows overwriting existing functions
- : Qualified name of the function as a list of strings (schema.function_name)
- : List of FunctionParameter nodes defining the function's parameter list
- : TypeName node specifying the return type (NULL for procedures)
- : List of DefElem nodes containing function options (LANGUAGE, VOLATILITY, SECURITY, etc.)
- : Parse tree containing the function body for SQL-language functions

## Dependencies
- Functions called/Symbols referenced:
  - TypeName (for return type specification)
  - FunctionParameter (for parameter definitions)
  - DefElem (for function options)
  - NodeTag (parse tree infrastructure)
  - List (PostgreSQL list structure)

- Called from (representative examples):
  - CreateFunction (main function creation handler)
  - ProcessUtilitySlow (utility command dispatcher)
  - CreateCommandTag (command tagging for logging)

## Notes and Other Information
- Handles both CREATE FUNCTION and CREATE PROCEDURE statements through is_procedure flag
- The replace flag implements CREATE OR REPLACE functionality
- Options list contains language specification, volatility settings, security context, cost estimates, etc.
- SQL-language functions store their body in sql_body field as a parse tree
- Other language functions specify their implementation through the options list
- Return type is NULL for procedures since they don't return values
- Part of PostgreSQL's extensibility framework for user-defined functions
- Used extensively in stored procedure and function definition processing