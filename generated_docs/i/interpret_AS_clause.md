# interpret_AS_clause

## Location
[src/backend/commands/functioncmds.c:851-1010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L851-L1010)

## Overview
Processes and validates the AS clause of CREATE FUNCTION/PROCEDURE statements, handling different function body formats based on the programming language.

## Definition
```c
static void interpret_AS_clause(Oid languageOid, const char *languageName,
                               char *funcname, List *as, Node *sql_body_in,
                               List *parameterTypes, List *inParameterNames,
                               char **prosrc_str_p, char **probin_str_p,
                               Node **sql_body_out,
                               const char *queryString)
```

## Detailed Description
This static function interprets the AS clause of function definitions differently based on the target language. For C language functions, it handles object file names and optional link symbol names. For SQL functions with unquoted bodies, it performs comprehensive parsing and transformation of the SQL statements. For other languages, it stores the function body as a string.

The function performs extensive validation including checking for duplicate or missing function bodies, ensuring SQL bodies are only used with SQL language, validating polymorphic argument restrictions, and transforming SQL statements through the parser. It sets up specialized parsing context for SQL functions using sql_fn_parser_setup().

## Parameters / Member Variables
- `languageOid`: OID of the function's programming language
- `languageName`: String name of the programming language
- `funcname`: Name of the function being created
- `as`: List containing AS clause elements (file names, function bodies, etc.)
- `sql_body_in`: Input SQL body node for unquoted SQL functions
- `parameterTypes`: List of parameter type OIDs
- `inParameterNames`: List of parameter names
- `prosrc_str_p`: Output pointer for function source code string
- `probin_str_p`: Output pointer for binary/object file path
- `sql_body_out`: Output pointer for processed SQL body node
- `queryString`: Original CREATE FUNCTION query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - strVal (extracts string values from nodes)
  - linitial, lsecond (list access functions)
  - [list_length](../l/list_length.md), list_nth, list_nth_oid (list utility functions)
  - [SQLFunctionParseInfo](../S/SQLFunctionParseInfo.md) (structure type for SQL function parsing)
  - IsPolymorphicType (checks if type is polymorphic)
  - [make_parsestate](../m/make_parsestate.md), free_parsestate (parser state management)
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md) (sets up SQL function parsing context)
  - [transformStmt](../t/transformStmt.md) (transforms parsed statements)
  - [GetCommandTagName](../G/GetCommandTagName.md), CreateCommandTag (command type utilities)
  - [pstrdup](../p/pstrdup.md) (string duplication)
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md) (src/backend/commands/functioncmds.c:1222)

## Notes and Other Information
- Handles three distinct cases: C language (object files), SQL language with unquoted bodies, and other languages (string bodies)
- For C functions, supports both explicit link symbol names and automatic function name substitution
- Maintains backward compatibility with PostgreSQL versions before 8.4 by handling "-" as omitted link symbol
- For SQL functions, validates against polymorphic arguments and utility statements in unquoted bodies
- Provides comprehensive error reporting with appropriate error codes and messages
- Sets up proper parsing context for SQL functions to handle parameter references correctly
- Part of PostgreSQL's multi-language function definition system supporting C, SQL, PL/pgSQL, and other procedural languages

## Simplified Source
```c
static void
interpret_AS_clause(Oid languageOid, const char *languageName,
                    char *funcname, List *as, Node *sql_body_in,
                    List *parameterTypes, List *inParameterNames,
                    char **prosrc_str_p, char **probin_str_p,
                    Node **sql_body_out, const char *queryString)
{
    // Basic validation - need either AS clause or SQL body, not both
    if (!sql_body_in && !as)
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                       errmsg("no function body specified")));
    if (sql_body_in && as)
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                       errmsg("duplicate function body specified")));
    if (sql_body_in && languageOid != SQLlanguageId)
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                       errmsg("inline SQL function body only valid for language SQL")));

    *sql_body_out = NULL;

    if (languageOid == ClanguageId) {
        // C language: store file name in probin, link symbol in prosrc
        *probin_str_p = strVal(linitial(as));
        if (list_length(as) == 1)
            *prosrc_str_p = funcname;  // Use function name as link symbol
        else {
            *prosrc_str_p = strVal(lsecond(as));
            if (strcmp(*prosrc_str_p, "-") == 0)  // Backward compatibility
                *prosrc_str_p = funcname;
        }
    }
    else if (sql_body_in) {
        // SQL language with unquoted body - parse and transform
        SQLFunctionParseInfoPtr pinfo = palloc0(sizeof(SQLFunctionParseInfo));

        // Set up parsing context
        pinfo->fname = funcname;
        pinfo->nargs = list_length(parameterTypes);
        pinfo->argtypes = (Oid *) palloc(pinfo->nargs * sizeof(Oid));
        pinfo->argnames = (char **) palloc(pinfo->nargs * sizeof(char *));

        // Extract parameter info
        for (int i = 0; i < list_length(parameterTypes); i++) {
            char *s = strVal(list_nth(inParameterNames, i));
            pinfo->argtypes[i] = list_nth_oid(parameterTypes, i);

            if (IsPolymorphicType(pinfo->argtypes[i]))
                ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                               errmsg("SQL function with unquoted function body cannot have polymorphic arguments")));

            pinfo->argnames[i] = (s[0] != '\0') ? s : NULL;
        }

        // Transform SQL statements
        if (IsA(sql_body_in, List)) {
            // Multiple statements
            List *stmts = linitial_node(List, castNode(List, sql_body_in));
            List *transformed_stmts = NIL;

            foreach(lc, stmts) {
                Node *stmt = lfirst(lc);
                ParseState *pstate = make_parsestate(NULL);
                pstate->p_sourcetext = queryString;
                sql_fn_parser_setup(pstate, pinfo);

                Query *q = transformStmt(pstate, stmt);
                if (q->commandType == CMD_UTILITY)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("%s is not yet supported in unquoted SQL function body",
                                         GetCommandTagName(CreateCommandTag(q->utilityStmt)))));

                transformed_stmts = lappend(transformed_stmts, q);
                free_parsestate(pstate);
            }
            *sql_body_out = (Node *) list_make1(transformed_stmts);
        }
        else {
            // Single statement
            ParseState *pstate = make_parsestate(NULL);
            pstate->p_sourcetext = queryString;
            sql_fn_parser_setup(pstate, pinfo);

            Query *q = transformStmt(pstate, sql_body_in);
            if (q->commandType == CMD_UTILITY)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("%s is not yet supported in unquoted SQL function body",
                                     GetCommandTagName(CreateCommandTag(q->utilityStmt)))));

            free_parsestate(pstate);
            *sql_body_out = (Node *) q;
        }

        *prosrc_str_p = pstrdup("");  // Empty string for SQL body functions
        *probin_str_p = NULL;
    }
    else {
        // Other languages: store function body as string
        *prosrc_str_p = strVal(linitial(as));
        *probin_str_p = NULL;

        if (list_length(as) != 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                           errmsg("only one AS item needed for language \"%s\"",
                                 languageName)));

        // Handle INTERNAL language backward compatibility
        if (languageOid == INTERNALlanguageId) {
            if (strlen(*prosrc_str_p) == 0)
                *prosrc_str_p = funcname;
        }
    }
}
```