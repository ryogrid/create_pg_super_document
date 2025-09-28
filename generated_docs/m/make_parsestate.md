# make_parsestate

## Location
[src/backend/parser/parse_node.c:39-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_node.c#L39-L71)

## Overview
Allocates and initializes a new ParseState structure for SQL parsing operations, with optional inheritance from a parent ParseState.

## Definition

```c
ParseState *
make_parsestate(ParseState *parentParseState)
```
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

## Simplified Source

```c
// Simplified version of make_parsestate
ParseState *make_parsestate(ParseState *parentParseState) {
    ParseState *pstate;

    // Allocate and zero-initialize the ParseState
    pstate = palloc0(sizeof(ParseState));

    // Set parent relationship
    pstate->parentParseState = parentParseState;

    // Initialize default values
    pstate->p_next_resno = 1;
    pstate->p_resolve_unknowns = true;

    // Inherit configuration from parent if provided
    if (parentParseState) {
        pstate->p_sourcetext = parentParseState->p_sourcetext;

        // Copy all hooks from parent
        pstate->p_pre_columnref_hook = parentParseState->p_pre_columnref_hook;
        pstate->p_post_columnref_hook = parentParseState->p_post_columnref_hook;
        pstate->p_paramref_hook = parentParseState->p_paramref_hook;
        pstate->p_coerce_param_hook = parentParseState->p_coerce_param_hook;
        pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;

        // Share query environment
        pstate->p_queryEnv = parentParseState->p_queryEnv;
    }

    return pstate;
}
```

Key simplifications made:
- Preserved essential ParseState initialization
- Maintained parent inheritance logic
- Kept hook copying for extensibility
- Focused on core parsing context setup