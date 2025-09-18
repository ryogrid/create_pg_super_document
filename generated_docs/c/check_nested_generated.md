# check_nested_generated

## Location
src/backend/catalog/heap.c: 2788 - 2805

## Overview
A static wrapper function that initiates validation of generated column expressions to ensure they do not contain references to other generated columns or invalid constructs.

## Definition
static void check_nested_generated(ParseState *pstate, Node *node)

## Detailed Description
This function serves as the entry point for validating expressions used in generated column definitions. It acts as a simple wrapper that calls the check_nested_generated_walker function to perform the actual recursive validation of the expression tree. The function ensures that generated columns maintain proper dependency relationships and do not create circular or invalid references within the PostgreSQL system.

The validation is crucial for maintaining data integrity and preventing situations where generated columns could depend on other generated columns, which would create complex dependency chains that could lead to inconsistent or undefined behavior.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parser state information and context for error reporting
- `node`: The root Node of the expression tree to be validated for generated column references

## Dependencies
- Functions called/Symbols referenced:
  - [check_nested_generated_walker](check_nested_generated_walker.md): The worker function that performs the recursive tree traversal and validation

- Called from (representative examples):
  - [cookDefault](cookDefault.md): Called during default value processing for generated columns

## Notes and Other Information
- This is a static function used internally within heap.c as part of the generated column validation system
- The function provides a clean interface to the more complex walker-based validation logic
- It follows PostgreSQL's pattern of having simple wrapper functions that delegate to more complex worker functions
- The validation occurs during column definition processing to catch invalid references early in the DDL process
- This function is part of PostgreSQL's generated column feature implementation