# ProcedureCreate

## Location
src/backend/catalog/pg_proc.c: 70 - 724

## Overview
Creates a new function/procedure in the PostgreSQL catalog (pg_proc table) or replaces an existing one, handling all validation, dependency tracking, and ACL setup.

## Definition


## Detailed Description
ProcedureCreate is the core function responsible for creating or updating function/procedure definitions in PostgreSQL's system catalog. It performs extensive validation of parameters, handles polymorphic and internal types, manages dependencies, validates function signatures, and maintains proper ACL permissions.

The function handles both new function creation and replacement of existing functions (when replace=true). For replacements, it enforces strict compatibility rules to prevent breaking existing callers - return types cannot change, parameter names cannot be modified, and default parameter types must remain consistent.

Key operations include:
- Parameter validation and type checking for polymorphic and internal types
- Variadic parameter handling and validation
- Dependency recording for all referenced objects (types, languages, transforms, etc.)
- Function body validation using language-specific validators
- ACL (access control list) setup with default permissions
- Statistics initialization for the new function

## Parameters / Member Variables
- : Name of the function/procedure to create
- : OID of the namespace where the function will be created
- : Whether to replace an existing function with the same signature
- : Whether the function returns a set of values
- : OID of the function's return type
- : OID of the function owner
- : OID of the implementation language (SQL, C, etc.)
- : OID of the validator function for this language
- : Source code of the function
- : Binary/library path for compiled functions (NULL for SQL functions)
- : Parsed SQL body for SQL language functions
- : Function kind ('f'=function, 'p'=procedure, 'a'=aggregate, 'w'=window)
- : Whether function runs with definer's privileges
- : Whether function is guaranteed not to leak information
- : Whether function returns NULL on any NULL input
- : Volatility level ('i'=immutable, 's'=stable, 'v'=volatile)
- : Parallel safety ('s'=safe, 'r'=restricted, 'u'=unsafe)
- : Array of input parameter type OIDs
- : Array including all parameter types (IN, OUT, INOUT, VARIADIC)
- : Array of parameter modes (IN, OUT, INOUT, VARIADIC)
- : Array of parameter names
- : List of default value expressions for parameters
- : Array of transform types for this function
- : Configuration parameters for this function
- : OID of support function for this function
- : Estimated execution cost
- : Estimated number of result rows (for set-returning functions)

## Dependencies
- Functions called/Symbols referenced:
  - check_valid_polymorphic_signature: Validates polymorphic type usage
  - check_valid_internal_signature: Validates internal type usage
  - SearchSysCache3: Searches for existing function definition
  - object_ownercheck: Verifies ownership permissions
  - build_function_result_tupdesc_t: Builds tuple descriptor for RECORD return types
  - get_user_default_acl: Gets default ACL for the function
  - record_object_address_dependencies: Records all object dependencies
  - OidFunctionCall1: Calls language validator function
  - CommandCounterIncrement: Makes new tuple visible to validator

- Called from (representative examples):
  - CreateFunction: Main entry point for CREATE FUNCTION command
  - AggregateCreate: Creates the final function for aggregate definitions
  - makeRangeConstructors: Creates constructor functions for range types
  - makeMultirangeConstructors: Creates constructor functions for multirange types

## Notes and Other Information
- The function enforces strict backward compatibility when replacing existing functions to prevent breaking dependent objects like views and rules
- Polymorphic type validation ensures that polymorphic return types have corresponding polymorphic input parameters for type resolution
- Internal type usage is restricted to prevent unsafe operations with pseudo-types
- Function validation is performed using language-specific validator functions, but only when check_function_bodies GUC is enabled
- Variadic parameters must be the last input parameter and are validated for proper array type usage
- The function creates comprehensive dependency records to track all objects the function depends on
- Statistics are initialized for new functions to support query planning cost estimation