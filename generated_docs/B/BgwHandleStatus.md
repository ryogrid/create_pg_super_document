# BgwHandleStatus

## Location
[src/include/postmaster/bgworker.h:109-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/bgworker.h#L109-L111)

## Overview
BgwHandleStatus is an enumeration that represents the current state of a background worker process, used for tracking and monitoring worker lifecycle.

## Definition


## Detailed Description
The BgwHandleStatus enumeration provides a comprehensive way to track the current state of background worker processes throughout their lifecycle. This status information is crucial for applications and systems that need to monitor worker health, coordinate with worker processes, or make decisions based on worker availability. The enumeration covers all possible states from initial registration through normal termination or abnormal failure scenarios.

## Parameters / Member Variables
- : Indicates the worker process is currently running and operational
- : Worker has been registered but the postmaster hasn't launched it yet
- : Worker process has terminated, either normally or due to an error
- : The postmaster process died, making worker status indeterminate

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is an enum definition)
- Called from (representative examples):
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - [WaitForBackgroundWorkerStartup](../W/WaitForBackgroundWorkerStartup.md)
  - [WaitForBackgroundWorkerShutdown](../W/WaitForBackgroundWorkerShutdown.md)
  - WaitForParallelWorkersToAttach
  - WaitForParallelWorkersToExit

## Notes and Other Information
This enumeration is primarily used with BackgroundWorkerHandle structures to provide status information about dynamically registered workers. The BGWH_POSTMASTER_DIED state is particularly important for robust error handling, as it indicates that the monitoring infrastructure itself has failed. Applications using background workers should check this status before attempting operations that depend on worker availability. The status transitions follow a predictable pattern: NOT_YET_STARTED → STARTED → STOPPED, with POSTMASTER_DIED being possible from any state during abnormal shutdown scenarios.