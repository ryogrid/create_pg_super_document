# AlterFunction

## Location
[src/backend/commands/functioncmds.c:1343-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1343-L1520)

## Overview
Implements the ALTER FUNCTION utility command, allowing modification of function properties such as volatility, strictness, security, cost, and parallel execution settings.

## Definition

```c
ObjectAddress
AlterFunction(ParseState *pstate, AlterFunctionStmt *stmt)
```
## Detailed Description
AlterFunction processes ALTER FUNCTION statements to modify various attributes of existing functions or procedures. The function validates permissions, processes the requested changes, and updates the pg_proc catalog accordingly. Key capabilities include:

1. Permission validation - ensures the user owns the function
2. Function type validation - prevents altering aggregates 
3. Attribute processing - handles volatility, strictness, security definer, leakproof, cost, rows, support functions, and parallel execution settings
4. Configuration parameter updates - processes SET/RESET clauses for function-specific GUC settings
5. Dependency management - properly handles support function dependencies
6. Catalog updates - commits changes to the pg_proc system catalog

The function handles both regular functions and procedures, with appropriate validation for procedure-specific constraints.

## Parameters / Member Variables
- : Parse state containing parsing context and environment information
- : AlterFunctionStmt structure containing the function identifier and list of requested modifications

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - ObjectAddressSet
  - SearchSysCacheCopy1
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [compute_common_attribute](../c/compute_common_attribute.md)
  - [interpret_func_volatility](../i/interpret_func_volatility.md)
  - [interpret_func_support](../i/interpret_func_support.md)
  - [interpret_func_parallel](../i/interpret_func_parallel.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [update_proconfig_value](../u/update_proconfig_value.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1659)

## Notes and Other Information
- Excludes RENAME and OWNER operations which are handled by the generic ALTER framework
- Enforces superuser privilege requirement for leakproof function designation
- Validates cost and rows parameters for positive values
- Properly manages support function dependencies with changeDependencyFor/recordDependencyOn
- Uses heap_modify_tuple for efficient catalog updates when handling configuration parameters
- Invokes post-alter hooks for proper event trigger and extension handling
- The function comment warns against accessing procForm after heap_modify_tuple as it becomes a dangling pointer