# elapsed_time

## Location
src/backend/commands/explain.c: 1273 - 1291

## Overview
Computes the elapsed time in seconds between a given start timestamp and the current time.

## Definition
```c
static double elapsed_time(instr_time *starttime)
```

## Detailed Description
The `elapsed_time` function is a utility function that calculates the time difference between a provided start timestamp and the current system time. It uses PostgreSQL's instrumentation time infrastructure to perform high-precision timing measurements. The function captures the current time, subtracts the start time, and returns the result as a floating-point number representing seconds.

This function is commonly used in PostgreSQL's explain functionality and other performance monitoring contexts where precise timing measurements are required.

## Parameters / Member Variables
- `starttime`: Pointer to an instr_time structure containing the reference start timestamp

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT (macro)
  - INSTR_TIME_SUBTRACT (macro)
  - INSTR_TIME_GET_DOUBLE (macro)
  - [instr_time](../i/instr_time.md) (type)
- Called from (representative examples):
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [IsCheckpointOnSchedule](../I/IsCheckpointOnSchedule.md)

## Notes and Other Information
- Returns elapsed time as a double-precision floating-point value in seconds
- Uses PostgreSQL's portable instrumentation time macros for cross-platform compatibility
- The function modifies a local endtime variable but does not modify the input starttime
- Commonly used for performance measurements in explain plans and system monitoring
- The precision depends on the underlying system's timer resolution