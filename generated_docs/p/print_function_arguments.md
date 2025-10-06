# print_function_arguments

## Location
[src/backend/utils/adt/ruleutils.c:3252-3399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3252-L3399)

## Overview
A comprehensive static helper function that formats and appends function arguments to a StringInfo buffer, supporting various argument modes, defaults, and special handling for table functions and ordered-set aggregates.

## Definition
```c
static int print_function_arguments(StringInfo buf, HeapTuple proctup, bool print_table_args, bool print_defaults)
```

## Detailed Description
This core utility function handles the complex formatting of function arguments for various PostgreSQL contexts. It can selectively print table arguments vs. regular arguments, include or exclude parameter defaults, and handles special cases like ordered-set aggregates and procedures. The function processes argument modes (IN, OUT, INOUT, VARIADIC, TABLE), manages argument names and types, and formats default expressions when requested. It also implements special logic for ordered-set aggregates that require 'ORDER BY' insertion and variadic argument handling.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted arguments to
- `proctup`: HeapTuple containing the function's metadata from pg_proc
- `print_table_args`: If true, prints only TABLE mode arguments; if false, prints all other argument modes
- `print_defaults`: If true, includes DEFAULT clauses for arguments that have default values

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - [get_func_arg_info](../g/get_func_arg_info.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [list_head](../l/list_head.md)
  - PROKIND_AGGREGATE
  - Form_pg_aggregate
  - AGGKIND_IS_ORDERED_SET
  - PROARGMODE_IN/INOUT/OUT/VARIADIC/TABLE
  - PROKIND_PROCEDURE
  - [quote_identifier](../q/quote_identifier.md)
  - [lnext](../l/lnext.md)
  - [deparse_expression](../d/deparse_expression.md)
- Called from (representative examples):
  - [NameHashEntry](../N/NameHashEntry.md)
  - [pg_get_functiondef](pg_get_functiondef.md)
  - [pg_get_function_arguments](pg_get_function_arguments.md)
  - [pg_get_function_identity_arguments](pg_get_function_identity_arguments.md)
  - [print_function_rettype](print_function_rettype.md)

## Notes and Other Information
- Returns the number of arguments actually printed to the buffer
- Handles complex PostgreSQL-specific features like ordered-set aggregates with ORDER BY clauses
- Implements special argument mode handling for procedures to avoid SQL syntax ambiguity
- Processes argument defaults by parsing stored default expressions from pg_proc
- The function supports both table function argument formatting and regular function argument formatting
- Includes a 'nasty hack' for variadic ordered-set aggregates that requires printing the last argument twice
- Part of the core infrastructure for generating SQL DDL statements and function signatures
- The print_table_args parameter allows the same function to handle both regular arguments and table function column specifications

## Simplified Source

```c
static int print_function_arguments(StringInfo buf, HeapTuple proctup,
                                   bool print_table_args, bool print_defaults) {
    Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(proctup);
    int numargs;
    Oid *argtypes;
    char **argnames;
    char *argmodes;
    int insertorderbyat = -1;
    int argsprinted;
    int inputargno;
    int nlackdefaults;
    List *argdefaults = NIL;
    ListCell *nextargdefault = NULL;
    int i;

    // Get function argument information
    numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);

    // Handle argument defaults if requested
    nlackdefaults = numargs;
    if (print_defaults && proc->pronargdefaults > 0) {
        Datum proargdefaults = SysCacheGetAttr(PROCOID, proctup,
                                              Anum_pg_proc_proargdefaults, &isnull);
        if (!isnull) {
            char *str = TextDatumGetCString(proargdefaults);
            argdefaults = castNode(List, stringToNode(str));
            pfree(str);
            nextargdefault = list_head(argdefaults);
            nlackdefaults = proc->pronargs - list_length(argdefaults);
        }
    }

    // Check for ordered-set aggregates special handling
    if (proc->prokind == PROKIND_AGGREGATE) {
        HeapTuple aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(proc->oid));
        Form_pg_aggregate agg = (Form_pg_aggregate) GETSTRUCT(aggtup);
        if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
            insertorderbyat = agg->aggnumdirectargs;
        ReleaseSysCache(aggtup);
    }

    // Process each argument
    argsprinted = 0;
    inputargno = 0;
    for (i = 0; i < numargs; i++) {
        Oid argtype = argtypes[i];
        char *argname = argnames ? argnames[i] : NULL;
        char argmode = argmodes ? argmodes[i] : PROARGMODE_IN;
        const char *modename;
        bool isinput;

        // Determine argument mode and whether it's an input argument
        switch (argmode) {
            case PROARGMODE_IN:
                modename = (proc->prokind == PROKIND_PROCEDURE) ? "IN " : "";
                isinput = true;
                break;
            case PROARGMODE_INOUT:
                modename = "INOUT ";
                isinput = true;
                break;
            case PROARGMODE_OUT:
                modename = "OUT ";
                isinput = false;
                break;
            case PROARGMODE_VARIADIC:
                modename = "VARIADIC ";
                isinput = true;
                break;
            case PROARGMODE_TABLE:
                modename = "";
                isinput = false;
                break;
            default:
                elog(ERROR, "invalid parameter mode '%c'", argmode);
        }

        if (isinput)
            inputargno++;

        // Skip if not the right type of argument we're printing
        if (print_table_args != (argmode == PROARGMODE_TABLE))
            continue;

        // Add ORDER BY for ordered-set aggregates
        if (argsprinted == insertorderbyat) {
            if (argsprinted)
                appendStringInfoChar(buf, ' ');
            appendStringInfoString(buf, "ORDER BY ");
        } else if (argsprinted) {
            appendStringInfoString(buf, ", ");
        }

        // Format the argument: mode + name + type + default
        appendStringInfoString(buf, modename);
        if (argname && argname[0])
            appendStringInfo(buf, "%s ", quote_identifier(argname));
        appendStringInfoString(buf, format_type_be(argtype));

        if (print_defaults && isinput && inputargno > nlackdefaults) {
            Node *expr = (Node *) lfirst(nextargdefault);
            nextargdefault = lnext(argdefaults, nextargdefault);
            appendStringInfo(buf, " DEFAULT %s",
                           deparse_expression(expr, NIL, false, false));
        }
        argsprinted++;

        // Special handling for variadic ordered-set aggregates
        if (argsprinted == insertorderbyat && i == numargs - 1) {
            i--;
            print_defaults = false;
        }
    }

    return argsprinted;
}
```