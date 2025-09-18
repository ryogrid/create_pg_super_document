# InjectionPointCondition

## Location
src/test/modules/injection_points/injection_points.c: 55 - 62

## Overview
A structure that defines runtime conditions for controlling when injection points are allowed to execute, primarily used for testing and debugging purposes.

## Definition


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
  - InjectionPointConditionType
- Called from (representative examples):
  - injection_point_allowed
  - injection_error
  - injection_notice
  - injection_wait
  - injection_points_attach

## Notes and Other Information
- This structure is part of the testing infrastructure located in 
- The condition evaluation is performed by the  function which checks the condition type and applies the appropriate logic
- When  is , the injection point will only execute if the current process ID () matches the  field
- This mechanism enables targeted testing where injection points can be restricted to specific processes, useful for testing multi-process scenarios in PostgreSQL