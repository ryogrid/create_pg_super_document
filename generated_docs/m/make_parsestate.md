# make_parsestate

## Location
src/backend/parser/parse_node.c: 39 - 71

## Overview
Allocates and initializes a new ParseState structure for SQL parsing operations, with optional inheritance from a parent ParseState.

## Definition


## Detailed Description
The  function creates a new ParseState structure that serves as the central context for SQL parsing operations in PostgreSQL. It allocates memory using  to ensure all fields start with zero/null values, then initializes critical fields and optionally inherits configuration from a parent ParseState.

The function establishes the foundation for parsing by setting up default values for resolution numbering () and enabling unknown type resolution (). When a parent ParseState is provided, the function creates a hierarchical parsing context by copying source text, hook functions, and query environment settings.

## Parameters / Member Variables
- : Optional parent ParseState to inherit configuration from. When non-NULL, the new ParseState inherits source text, column reference hooks, parameter hooks, coercion hooks, hook state, and query environment.

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
- Called from (representative examples):
  -  (src/backend/parser/analyze.c:108)
  -  (src/backend/parser/analyze.c:148)
  -  (src/backend/parser/analyze.c:226)
  -  (src/backend/parser/analyze.c:696)
  -  (src/backend/commands/policy.c:619)
  -  (src/backend/commands/tablecmds.c:1096)

## Notes and Other Information
- Memory is allocated using , ensuring all fields start with zero/null values
- The caller is responsible for eventually releasing the ParseState via 
- Inheritance from parent ParseState enables nested parsing contexts, commonly used in subqueries and complex SQL constructs
- Hook functions allow customization of parsing behavior for different contexts
- Location: src/backend/parser/parse_node.c:39-71