# injection_point_allowed

## Location
[src/test/modules/injection_points/injection_points.c:137-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L137-L158)

## Overview
A static function that evaluates runtime conditions to determine whether a named injection point should be allowed to execute.

## Definition

```c
static bool
injection_point_allowed(InjectionPointCondition *condition)
```
## Detailed Description
This function implements the conditional logic for injection points in PostgreSQL's testing framework. It takes an  structure and evaluates the specified conditions to determine if the injection point should be triggered. The function supports different condition types including process ID-based filtering and always-allow conditions. This allows for fine-grained control over when injection points are activated, enabling targeted testing scenarios.

## Parameters / Member Variables
- `*condition`: Pointer to an  structure containing the condition type and associated parameters to evaluate
## Dependencies
- Functions called/Symbols referenced:
  -  (struct type defining condition parameters)
  -  (enum value for process ID-based conditions)
  -  (enum value for always-allow conditions)
  -  (global variable containing current process ID)
- Called from:
  -  (at src/test/modules/injection_points/injection_points.c:182)
  -  (at src/test/modules/injection_points/injection_points.c:193)  
  -  (at src/test/modules/injection_points/injection_points.c:211)

## Notes and Other Information
- This is a static function, only accessible within injection_points.c
- Currently supports two condition types:
  - : Only allows execution if current process ID matches the specified PID
  - : Always allows execution (no filtering)
- Returns  if the injection point should execute,  otherwise
- The function uses a switch statement pattern that can be easily extended for additional condition types
- Part of PostgreSQL's testing infrastructure for creating controlled test environments

## Simplified Source

```c
static bool
injection_point_allowed(InjectionPointCondition *condition)
{
    bool result = true;

    // Check condition type and evaluate accordingly
    switch (condition->type)
    {
        case INJ_CONDITION_PID:
            // Only allow if current process matches target PID
            if (MyProcPid != condition->pid)
                result = false;
            break;

        case INJ_CONDITION_ALWAYS:
            // Always allow execution
            break;
    }

    return result;
}
```