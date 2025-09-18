# is_true_boolean_expression

## Location
[src/bin/psql/command.c:3173-3189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3173-L3189)

## Overview
Evaluates boolean expressions from psql conditional commands, returning true only if the expression is both valid and evaluates to true.

## Definition
```c
static bool is_true_boolean_expression(PsqlScanState scan_state, const char *name)
```

## Detailed Description
This function serves as the core evaluation engine for psql conditional commands like `\if` and `\elif`. It first uses `gather_boolean_expression()` to collect all command-line arguments into a single expression string, then passes this expression to `ParseVariableBool()` for parsing and evaluation. The function implements a strict two-phase validation: the expression must be syntactically valid AND evaluate to true for the function to return true.

The function is designed to work within psqls conditional execution stack, where variable expansion and backtick substitution require the conditional stacks top state to be active. This ensures proper evaluation of dynamic expressions containing psql variables.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current parsing state for extracting the boolean expression
- `name`: String identifier used for error reporting when the boolean parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - [gather_boolean_expression](../g/gather_boolean_expression.md) (to collect command arguments into expression string)
  - [ParseVariableBool](../P/ParseVariableBool.md) (to parse and evaluate the gathered boolean expression)
  - destroyPQExpBuffer (to clean up the expression buffer)
- Called from (representative examples):
  - [exec_command_if](../e/exec_command_if.md) (to evaluate conditions in `\if` commands)
  - [exec_command_elif](../e/exec_command_elif.md) (to evaluate conditions in `\elif` commands)

## Notes and Other Information
- Returns true only when both parsing succeeds AND the expression evaluates to true
- Returns false for invalid expressions, expressions that evaluate to false, or parsing errors
- Requires the conditional stacks top state to be active for proper variable expansion
- Handles memory management by destroying the PQExpBuffer after evaluation
- Central component of psqls conditional command infrastructure
- The name parameter is passed through to ParseVariableBool for error context
- Part of the psql conditional execution system supporting complex scripting scenarios