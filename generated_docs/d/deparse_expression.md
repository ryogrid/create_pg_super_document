# deparse_expression

## Location
src/backend/utils/adt/ruleutils.c: 3599 - 3625

## Overview
A general utility function for deparsing expressions that calls deparse_expression_pretty with all pretty printing disabled.

## Definition


## Detailed Description
This function serves as a simplified wrapper around deparse_expression_pretty, providing a convenient interface for expression deparsing when pretty printing is not needed. It converts a PostgreSQL expression node tree back into its string representation using the provided deparse context. The function always calls deparse_expression_pretty with prettyFlags=0 and startIndent=0, effectively disabling all formatting enhancements.

## Parameters / Member Variables
- : The node tree to be deparsed. Must be a transformed expression tree (not raw gram.y output)
- : A list of deparse_namespace nodes representing the context for interpreting Vars in the node tree. Can be NIL if no Vars are expected
- : When true, forces all Vars to be prefixed with their table names
- : When true, forces all implicit casts to be shown explicitly

## Dependencies
- Functions called/Symbols referenced:
  - deparse_expression_pretty
- Called from (representative examples):
  - show_plan_tlist (src/backend/commands/explain.c:2475)
  - show_expression (src/backend/commands/explain.c:2500)
  - show_grouping_set_keys (src/backend/commands/explain.c:2715)
  - DefineDomain (src/backend/commands/typecmds.c:929)
  - pg_get_partconstrdef_string (src/backend/utils/adt/ruleutils.c:2116)

## Notes and Other Information
This function is primarily used when a simple string representation of an expression is needed without concern for formatting or readability. For formatted output suitable for display, use deparse_expression_pretty directly with appropriate pretty printing flags.