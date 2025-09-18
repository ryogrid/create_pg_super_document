# injection_notice

## Location
src/test/modules/injection_points/injection_points.c: 189 - 200

## Overview
A callback function that logs a NOTICE message when an injection point is triggered, used for testing and debugging injection point functionality.

## Definition
void injection_notice(const char *name, const void *private_data)

## Detailed Description
The `injection_notice` function serves as a callback action for injection points that outputs diagnostic notice messages. It's part of PostgreSQL's injection points testing framework, designed to provide visible confirmation when specific injection points are executed during testing scenarios. The function first validates whether the injection point should be triggered based on the provided condition, and if allowed, emits a NOTICE-level log message containing the injection point's name.

## Parameters / Member Variables
- `name`: The name identifier of the injection point being triggered
- `private_data`: A pointer to an InjectionPointCondition structure that contains filtering criteria for determining whether the injection point should execute

## Dependencies
- Functions called/Symbols referenced:
  - [injection_point_allowed](injection_point_allowed.md)
  - elog
  - [InjectionPointCondition](../I/InjectionPointCondition.md)
  - NOTICE
- Called from (representative examples):
  - No direct callers found (used as callback function name string in injection_points_attach)

## Notes and Other Information
- This function is typically registered as a callback through the injection_points_attach function by specifying "notice" as the action parameter
- The function provides a non-disruptive way to verify injection point execution during testing
- Uses NOTICE log level which is visible to clients by default, making it suitable for test verification
- Part of the injection_points test module located in src/test/modules/injection_points/