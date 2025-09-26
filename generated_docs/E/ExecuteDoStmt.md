# ExecuteDoStmt

## Location
[src/backend/commands/functioncmds.c:2066-2187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L2066-L2187)

## Overview
Executes inline procedural-language code blocks specified in DO statements, handling language validation, permission checks, and code execution.

## Definition
```c
void ExecuteDoStmt(ParseState *pstate, DoStmt *stmt, bool atomic)
```

## Detailed Description
This function implements PostgreSQL's DO statement functionality, which allows execution of inline procedural language code without creating a persistent function. It processes the DO statement's options (AS and LANGUAGE), validates the specified procedural language exists and supports inline execution, performs appropriate permission checks based on whether the language is trusted or untrusted, and then invokes the language's inline handler to execute the code.

The function creates an InlineCodeBlock structure containing the source code and metadata, then calls the language's inline handler function. It supports both trusted languages (requiring USAGE privilege) and untrusted languages (requiring superuser privilege). The atomic parameter controls transaction behavior during execution.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and context information
- `stmt`: DoStmt node containing the parsed DO statement with its options
- `atomic`: Boolean flag controlling whether the execution should be atomic (affects transaction handling)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (node creation)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md) (error reporting)
  - strVal (string value extraction)
  - [SearchSysCache1](../S/SearchSysCache1.md) (language lookup)
  - [extension_file_exists](../e/extension_file_exists.md) (extension existence check)
  - [object_aclcheck](../o/object_aclcheck.md) (permission checking)
  - [superuser](../s/superuser.md) (superuser check)
  - [aclcheck_error](../a/aclcheck_error.md) (permission error reporting)
  - OidFunctionCall1 (inline handler execution)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Defaults to 'plpgsql' language if no LANGUAGE option is specified
- Requires USAGE privilege for trusted languages, superuser privilege for untrusted languages
- Validates that the language supports inline code execution via laninline handler
- Creates InlineCodeBlock with source text, language OID, trust status, and atomic flag
- Part of PostgreSQL's procedural language infrastructure
- Provides helpful error hints when language extensions are not loaded
- Used for one-time code execution without creating permanent database objects