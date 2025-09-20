# VariableSetKind

## Location
[src/include/nodes/parsenodes.h:2616-2617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2616-L2617)

## Overview
VariableSetKind is an enumeration that defines the different types of variable setting operations in PostgreSQL SET and RESET statements.

## Definition

```c
typedef struct VariableSetStmt
{
	NodeTag		type;
	VariableSetKind kind;
	char	   *name;			/* variable to be set */
	List	   *args;			/* List of A_Const nodes */
	bool		is_local;		/* SET LOCAL? */
} VariableSetStmt;
```
## Detailed Description
This enumeration distinguishes between different syntactic forms of PostgreSQL's SET and RESET statements for configuration parameters. While some of these operations are semantically equivalent (such as "SET var TO DEFAULT" and "RESET var"), the distinction is preserved to support proper command tag creation and to handle different parsing contexts.

The enum covers various ways of setting configuration variables: assigning specific values, resetting to defaults, copying from session variables, handling complex multi-parameter transactions, and resetting individual or all variables. This classification is essential for the parser and command execution system to handle the diverse syntax variations of variable manipulation commands.

## Parameters / Member Variables
- : Standard variable assignment using the "SET variable = value" syntax. This is the most common form of setting configuration parameters to specific values.
- : Setting a variable to its default value using "SET variable TO DEFAULT" syntax. While semantically equivalent to RESET, it's distinguished for command tag purposes.
- : Setting a variable using the current session's value with "SET variable FROM CURRENT" syntax. This allows copying the current session value to the local transaction scope.
- : Special case for complex SET TRANSACTION statements that can set multiple transaction characteristics in a single command (e.g., isolation level, read-only mode, deferrable mode).
- : Resetting a single variable to its default value using "RESET variable" syntax. Functionally equivalent to VAR_SET_DEFAULT but uses different SQL syntax.
- : Resetting all configuration parameters to their default values using "RESET ALL" syntax. This is a bulk operation affecting all settable parameters.

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Called from (representative examples):
  -  structure in src/include/nodes/parsenodes.h:2621

## Notes and Other Information
- This enum is defined in src/include/nodes/parsenodes.h:2608-2616
- The enum is used as the  field in the  structure to specify what type of variable operation is being performed
- The distinction between VAR_SET_DEFAULT and VAR_RESET is preserved specifically for CreateCommandTag() functionality, even though they are semantically equivalent
- VAR_SET_MULTI handles the complex case of SET TRANSACTION statements which can modify multiple transaction properties simultaneously
- The enum supports both session-level and transaction-local variable modifications through the VariableSetStmt structure
- Each enum value corresponds to different SQL syntax patterns for configuration parameter management