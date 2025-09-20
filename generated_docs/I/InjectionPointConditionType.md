# InjectionPointConditionType

## Location
[src/test/modules/injection_points/injection_points.c:53-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L53-L54)

## Overview
An enumerated type that defines the different types of runtime conditions that can be applied to injection points in PostgreSQL's testing framework.

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
 is an enumeration that specifies the types of conditions under which injection points can be triggered. This enum is part of PostgreSQL's injection point testing infrastructure, which allows developers to inject user-defined callbacks at predetermined code paths for testing purposes. The enum currently supports two condition types: unconditional execution and PID-based restrictions.

## Parameters / Member Variables
- `type`: Value 0, indicates that the injection point should always run regardless of any conditions
- `pid`: Indicates that the injection point should only run when specific process ID conditions are met
## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enum definition)
- Used by:
  - [InjectionPointCondition](InjectionPointCondition.md) struct (as the  field)

## Notes and Other Information
- This enum is defined in the test module for injection points ()
- The enum is designed to be extensible, as indicated by the comment "If more types of runtime conditions need to be tracked, this structure should be expanded"
- Currently supports only two condition types but can be extended to add more sophisticated condition checking mechanisms
- Part of PostgreSQL's testing infrastructure, specifically for injection point functionality that enables controlled testing of various code paths
- The enum values are used in conjunction with the  structure to determine when and how injection points should be triggered