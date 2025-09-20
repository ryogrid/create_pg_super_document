# Variable

## Location
[src/bin/pgbench/pgbench.c:327-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L327-L344)

## Overview
The Variable structure represents an individual variable in pgbench's variable management system, storing both string and typed representations of variable values.

## Definition

```c
structure for client variables.
 */
typedef struct
{
	Variable   *vars;			/* array of variable definitions */
	int			nvars;			/* number of variables */

	/*
	 * The maximum number of variables that we can currently store in 'vars'
	 * without having to reallocate more space. We must always have max_vars
	 * >= nvars.
	 */
	int			max_vars;

	bool		vars_sorted;	/* are variables sorted by name? */
} Variables;
```
## Detailed Description
The Variable structure is a core data type in pgbench that represents individual variables used in benchmark scripts and expressions. Each variable maintains both a string representation (svalue) and a typed value (PgBenchValue) to support flexible variable usage throughout the benchmarking process. 

The structure is designed to handle lazy evaluation where the string form may be computed on demand, indicated by the possibility of svalue being NULL when not yet calculated. This dual representation allows efficient operations while maintaining compatibility with both string-based script operations and typed mathematical computations.

## Parameters / Member Variables
- `name`: Pointer to the variable's name as a null-terminated string
- `svalue`: String representation of the variable's value, may be NULL if not yet computed
- `value`: The actual typed value stored as a PgBenchValue union structure containing integer, double, or boolean values

## Dependencies
- Functions called/Symbols referenced:
  - PgBenchValue (union type for typed values)
- Called from (representative examples):
  - [compareVariableNames](../c/compareVariableNames.md) (for variable comparison operations)
  - [lookupVariable](../l/lookupVariable.md) (for variable lookup operations)
  - [getVariable](../g/getVariable.md) (for variable retrieval)
  - [putVariable](../p/putVariable.md) (for variable assignment)
  - [evaluateExpr](../e/evaluateExpr.md) (for expression evaluation)
  - [enlargeVariables](../e/enlargeVariables.md) (for dynamic array expansion)
  - [makeVariableValue](../m/makeVariableValue.md) (for value construction)

## Notes and Other Information
- Located in src/bin/pgbench/pgbench.c at lines 322-327
- Used as the element type in Variables structure arrays for client variable storage
- Supports lazy string conversion where svalue may be NULL until string representation is needed
- The PgBenchValue member can store int64, double, or boolean values through its union structure
- Part of pgbench's comprehensive variable management system for benchmark script execution
- [Variables](Variables.md) are typically managed in sorted arrays for efficient lookup operations