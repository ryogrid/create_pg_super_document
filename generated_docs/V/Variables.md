# Variables

## Location
[src/bin/pgbench/pgbench.c:345-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L345-L346)

## Overview
The Variables structure is a container that manages a dynamic array of Variable instances for client-side variable storage in pgbench, providing efficient organization and lookup capabilities.

## Definition

```c
structure to keep stats about something.
 *
 * XXX probably the first value should be kept and used as an offset for
 * better numerical stability...
 */
typedef struct SimpleStats
{
	int64		count;			/* how many values were encountered */
	double		min;			/* the minimum seen */
	double		max;			/* the maximum seen */
	double		sum;			/* sum of values */
	double		sum2;			/* sum of squared values */
} SimpleStats;
```
## Detailed Description
The Variables structure serves as a comprehensive variable management container in pgbench, implementing a dynamic array of Variable structures with built-in capacity management and optional sorting. This design allows efficient variable storage, lookup, and manipulation during benchmark execution.

The structure maintains both the current count (nvars) and maximum capacity (max_vars) to support dynamic growth without frequent reallocations. The vars_sorted flag enables optimization of lookup operations by indicating whether the array is maintained in sorted order by variable name, allowing for binary search algorithms when appropriate.

This container is fundamental to pgbench's variable system, supporting script-based benchmarks that require variable storage and manipulation across multiple database operations.

## Parameters / Member Variables
- : Pointer to dynamically allocated array of Variable structures containing the actual variable data
- : Current number of variables stored in the array
- : Maximum number of variables that can be stored without reallocating the vars array (must always be >= nvars)
- : Boolean flag indicating whether the variables are sorted by name for optimized lookups

## Dependencies
- Functions called/Symbols referenced:
  - [Variable](Variable.md) (struct type for individual variables)
- Called from (representative examples):
  - [lookupVariable](../l/lookupVariable.md) (for variable search operations)
  - [getVariable](../g/getVariable.md) (for variable retrieval)
  - [enlargeVariables](../e/enlargeVariables.md) (for dynamic array expansion)
  - [lookupCreateVariable](../l/lookupCreateVariable.md) (for variable creation)
  - [putVariable](../p/putVariable.md) (for variable assignment)
  - [putVariableValue](../p/putVariableValue.md) (for value assignment)
  - [assignVariables](../a/assignVariables.md) (for bulk variable assignment)
  - [getQueryParams](../g/getQueryParams.md) (for parameter extraction)
  - [runShellCommand](../r/runShellCommand.md) (for shell command variable context)
  - [evaluateSleep](../e/evaluateSleep.md) (for sleep evaluation with variables)

## Notes and Other Information
- Located in src/bin/pgbench/pgbench.c at lines 332-345
- Part of pgbench's client state management system (referenced in ConnectionStateEnum)
- Designed for efficient memory management with capacity-based growth strategy
- Supports both sorted and unsorted modes for different performance characteristics
- Used extensively throughout pgbench for managing per-client variable state during benchmark execution
- The max_vars >= nvars invariant ensures memory safety and proper capacity management