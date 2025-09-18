# RecoveryRequiresIntParameter

## Location
[src/backend/access/transam/xlogrecovery.c:4660-4740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4660-L4740)

## Overview
RecoveryRequiresIntParameter validates that PostgreSQL server configuration parameters meet minimum requirements during recovery, pausing hot standby or terminating recovery if conditions are not met.

## Definition
```c
void RecoveryRequiresIntParameter(const char *param_name, int currValue, int minValue)
```

## Detailed Description
This function is a critical safety mechanism in PostgreSQL's recovery process that ensures configuration parameters on the standby server meet minimum requirements established by the primary server. When a parameter value on the standby is lower than required, the function provides different behaviors based on the recovery state:

1. **Hot Standby Active**: Issues warnings, pauses recovery, and enters a loop waiting for configuration correction or promotion trigger
2. **Non-Hot Standby**: Immediately terminates recovery with a FATAL error

The function implements a graceful degradation strategy - in hot standby mode, it allows administrators time to correct configuration issues before forcing termination.

## Parameters / Member Variables
- `param_name`: Name of the configuration parameter being validated (used in error messages)
- `currValue`: Current value of the parameter on the standby server
- `minValue`: Minimum required value as determined from the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [HotStandbyActiveInReplay](../H/HotStandbyActiveInReplay.md)
  - [SetRecoveryPause](../S/SetRecoveryPause.md)
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md)
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md)
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)
  - [ConfirmRecoveryPaused](../C/ConfirmRecoveryPaused.md)
  - [ConditionVariableTimedSleep](../C/ConditionVariableTimedSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - RECOVERY_NOT_PAUSED
- Called from:
  - [CheckRequiredParameterValues](../C/CheckRequiredParameterValues.md) (multiple parameter validations)

## Notes and Other Information
- The function never returns when currValue < minValue - it either pauses indefinitely or terminates with FATAL
- During the pause loop, it periodically checks for promotion triggers and handles startup interrupts
- Error messages include specific parameter names and values to aid in troubleshooting
- The 1000ms timeout in ConditionVariableTimedSleep allows periodic status checks while paused
- This is part of PostgreSQL's robust parameter consistency enforcement between primary and standby servers