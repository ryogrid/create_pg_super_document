# LookupFuncWithArgs

## Location
src/backend/parser/parse_func.c: 2206 - 2510

## Overview
LookupFuncWithArgs provides comprehensive function/procedure/aggregate lookup functionality using ObjectWithArgs structures, supporting both traditional input-only and modern input+output parameter matching for SQL standard compliance.

## Definition
```c
Oid LookupFuncWithArgs(ObjectType objtype, ObjectWithArgs *func, bool missing_ok)
```

## Detailed Description
This function implements the most sophisticated function lookup mechanism in PostgreSQL, supporting lookup of functions, procedures, aggregates, or any routine type. It handles ObjectWithArgs structures that can specify argument types with optional parameter modes. The function performs a two-stage lookup process: first using traditional PostgreSQL rules (input arguments only), then for procedures/routines, it attempts a second lookup including all parameters if no parameter modes are explicitly specified. This dual approach ensures SQL standard compliance for procedures while maintaining backward compatibility with traditional PostgreSQL function lookup semantics.

## Parameters / Member Variables
- `objtype`: Type of object to find (OBJECT_FUNCTION, OBJECT_PROCEDURE, OBJECT_AGGREGATE, or OBJECT_ROUTINE)
- `func`: ObjectWithArgs structure containing function name, argument types, and optionally full parameter specifications
- `missing_ok`: If true, return InvalidOid instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - LookupFuncNameInternal  
  - LookupTypeNameOid
  - get_func_prokind
  - list_length
  - lfirst_node
  - func_signature_string
  - NameListToString
  - ereport
  - errcode
  - errmsg
  - errhint
  - FUNC_MAX_ARGS
  - PROKIND_PROCEDURE
  - PROKIND_AGGREGATE
- Called from (representative examples):
  - DROP FUNCTION/PROCEDURE/AGGREGATE commands
  - ALTER FUNCTION/PROCEDURE commands
  - Various DDL statement parsers

## Notes and Other Information
The function implements complex logic to handle SQL standard procedure semantics where all parameters (IN, OUT, INOUT) are considered for matching, while preserving PostgreSQL's traditional function lookup that considers only input parameters. It validates that the found object matches the requested object type, with historical exceptions allowing functions to match aggregates and window functions. The two-stage lookup prevents ambiguity between functions and procedures with the same input signature but different output parameters. Maximum argument limits are enforced with appropriate error messages distinguishing between functions and procedures.