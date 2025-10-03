# deparse_expression

## Location
[src/backend/utils/adt/ruleutils.c:3599-3625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3599-L3625)

## Overview
A general utility function for deparsing expressions that calls deparse_expression_pretty with all pretty printing disabled.

## Definition

```c
char *
deparse_expression(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit)
```
## Detailed Description
This function serves as a simplified wrapper around deparse_expression_pretty, providing a convenient interface for expression deparsing when pretty printing is not needed. It converts a PostgreSQL expression node tree back into its string representation using the provided deparse context. The function always calls deparse_expression_pretty with prettyFlags=0 and startIndent=0, effectively disabling all formatting enhancements.

## Parameters / Member Variables
- `*expr`: The node tree to be deparsed. Must be a transformed expression tree (not raw gram.y output)
- `*dpcontext`: A list of deparse_namespace nodes representing the context for interpreting Vars in the node tree. Can be NIL if no Vars are expected
- `forceprefix`: When true, forces all Vars to be prefixed with their table names
- `showimplicit`: When true, forces all implicit casts to be shown explicitly
## Dependencies
- Functions called/Symbols referenced:
  - [deparse_expression_pretty](deparse_expression_pretty.md)
- Called from (representative examples):
  - [show_plan_tlist](../s/show_plan_tlist.md) (src/backend/commands/explain.c:2475)
  - [show_expression](../s/show_expression.md) (src/backend/commands/explain.c:2500)
  - [show_grouping_set_keys](../s/show_grouping_set_keys.md) (src/backend/commands/explain.c:2715)
  - [DefineDomain](../D/DefineDomain.md) (src/backend/commands/typecmds.c:929)
  - [pg_get_partconstrdef_string](../p/pg_get_partconstrdef_string.md) (src/backend/utils/adt/ruleutils.c:2116)

## Notes and Other Information
This function is primarily used when a simple string representation of an expression is needed without concern for formatting or readability. For formatted output suitable for display, use deparse_expression_pretty directly with appropriate pretty printing flags.

## Simplified Source
```c
char *
deparse_expression(Node *expr, List *dpcontext,
                   bool forceprefix, bool showimplicit)
{
    // Simple wrapper that disables pretty printing
    return deparse_expression_pretty(expr, dpcontext, forceprefix,
                                     showimplicit, 0, 0);
}
```