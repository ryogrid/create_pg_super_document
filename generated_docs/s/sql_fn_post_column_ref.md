# sql_fn_post_column_ref

## Location
[src/backend/executor/functions.c:278-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L278-L393)

## Overview
Parser callback function for handling ColumnRef nodes in SQL function bodies, resolving parameter names and field references.

## Definition

```c
static Node *
sql_fn_post_column_ref(ParseState *pstate, ColumnRef *cref, Node *var)
```
## Detailed Description
This function serves as a post-processing callback for column references encountered during SQL function parsing. It handles various syntactic forms of parameter references including simple parameter names, qualified parameter names with function name prefixes, field references within composite parameters, and whole-row references. The function implements PostgreSQL's parameter scoping rules where parameter names appear in a scope outside individual SQL commands, ensuring table-column references take precedence over parameter references.

## Parameters / Member Variables
- : ParseState containing parser context and hook state information
- : ColumnRef node representing the column reference to be processed
- : Existing variable node if already resolved (NULL if not resolved)

## Dependencies
- Functions called/Symbols referenced:
  - [ColumnRef](../C/ColumnRef.md)
  - SQLFunctionParseInfoPtr
  - [A_Star](../A/A_Star.md)
  - llast
  - lsecond
  - [sql_fn_resolve_param_name](sql_fn_resolve_param_name.md)
  - lthird
  - [String](../S/String.md)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
- Called from (representative examples):
  - [sql_fn_parser_setup](sql_fn_parser_setup.md) (src/backend/executor/functions.c:268)

## Notes and Other Information
- Supports multiple syntax forms: A (parameter name), A.B (function.parameter or parameter.field), A.B.C (function.parameter.field), A.* and A.B.* (whole-row references)
- Never overrides table-column references to maintain proper scoping semantics
- Handles both simple parameter references and composite parameter field access
- Uses ParseFuncOrColumn for resolving field references within composite parameters
- Returns NULL if no parameter match is found, allowing other resolution mechanisms to take precedence
- The function name qualification is optional but can be used to disambiguate parameter references

## Simplified Source

```c
static Node *sql_fn_post_column_ref(ParseState *pstate, ColumnRef *cref, Node *var) {
    SQLFunctionParseInfoPtr pinfo = (SQLFunctionParseInfoPtr) pstate->p_ref_hook_state;
    int nnames;
    Node *field1;
    Node *subfield = NULL;
    const char *name1;
    const char *name2 = NULL;
    Node *param;

    // Never override table-column references
    if (var != NULL)
        return NULL;

    // Handle different column reference formats:
    // A, A.B, A.B.C, A.*, A.B.*
    nnames = list_length(cref->fields);

    if (nnames > 3)
        return NULL;

    // Ignore trailing "*" for whole-row references
    if (IsA(llast(cref->fields), A_Star))
        nnames--;

    // Extract field names
    field1 = (Node *) linitial(cref->fields);
    name1 = strVal(field1);
    if (nnames > 1) {
        subfield = (Node *) lsecond(cref->fields);
        name2 = strVal(subfield);
    }

    if (nnames == 3) {
        // Three-part name: function.parameter.field
        if (strcmp(name1, pinfo->fname) != 0)
            return NULL;

        param = sql_fn_resolve_param_name(pinfo, name2, cref->location);
        subfield = (Node *) lthird(cref->fields);
        Assert(IsA(subfield, String));
    }
    else if (nnames == 2 && strcmp(name1, pinfo->fname) == 0) {
        // Two-part name starting with function name
        param = sql_fn_resolve_param_name(pinfo, name2, cref->location);

        if (param) {
            // function.parameter
            subfield = NULL;
        } else {
            // Try parameter.field instead
            param = sql_fn_resolve_param_name(pinfo, name1, cref->location);
        }
    }
    else {
        // Single name or parameter.field
        param = sql_fn_resolve_param_name(pinfo, name1, cref->location);
    }

    if (!param)
        return NULL;  // No parameter match

    // Handle field reference for composite parameters
    if (subfield) {
        param = ParseFuncOrColumn(pstate,
                                 list_make1(subfield),
                                 list_make1(param),
                                 pstate->p_last_srf,
                                 NULL,
                                 false,
                                 cref->location);
    }

    return param;
}
```