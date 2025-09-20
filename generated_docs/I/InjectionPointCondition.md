# InjectionPointCondition

## Location
[src/test/modules/injection_points/injection_points.c:55-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L55-L62)

## Overview
A structure that defines runtime conditions for controlling when injection points are allowed to execute, primarily used for testing and debugging purposes.

## Definition

```c
typedef struct InjectionPointCondition
{
	/* Type of the condition */
	InjectionPointConditionType type;

	/* ID of the process where the injection point is allowed to run */
	int			pid;
} InjectionPointCondition;
```
## Detailed Description
The  structure is part of PostgreSQL's injection point testing framework. It provides a mechanism to conditionally execute injection points based on runtime criteria. This allows fine-grained control over when testing code should be activated, enabling more precise testing scenarios where injection points should only fire under specific conditions.

The structure supports different types of conditions through the  field, and currently includes support for process ID-based restrictions through the  field. This enables testing scenarios where injection points should only be active in specific processes.

## Parameters / Member Variables
- : Specifies the type of condition to evaluate, using  enum values:
  - : The injection point always runs (no restrictions)
  - : The injection point only runs if the current process ID matches the specified PID
- : The process ID that must match the current process () for the injection point to be allowed to execute (only used when  is )

## Dependencies
- Functions called/Symbols referenced:
  - [InjectionPointConditionType](InjectionPointConditionType.md)
- Called from (representative examples):
  - [injection_point_allowed](../i/injection_point_allowed.md)
  - [injection_error](../i/injection_error.md)
  - [injection_notice](../i/injection_notice.md)
  - [injection_wait](../i/injection_wait.md)
  - [injection_points_attach](../i/injection_points_attach.md)

## Notes and Other Information
- This structure is part of the testing infrastructure located in 
- The condition evaluation is performed by the  function which checks the condition type and applies the appropriate logic
- When  is , the injection point will only execute if the current process ID () matches the  field
- This mechanism enables targeted testing where injection points can be restricted to specific processes, useful for testing multi-process scenarios in PostgreSQL