# ShutdownAuxiliaryProcess

## Location
[src/backend/postmaster/auxprocess.c:101-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/auxprocess.c#L101-L106)

## Overview
Shutdown callback function for auxiliary processes that performs essential cleanup operations, equivalent to ShutdownPostgres() but simplified for non-transactional auxiliary processes.

## Definition
static void ShutdownAuxiliaryProcess(int code, Datum arg)

## Detailed Description
ShutdownAuxiliaryProcess serves as the shutdown callback for auxiliary processes, providing a lightweight alternative to the full ShutdownPostgres() sequence used by regular backend processes. Since auxiliary processes don't run transactions, most of the complex transaction abort logic is unnecessary. However, the function ensures critical cleanup operations are performed, particularly releasing any held LWLocks which is essential during error exits to prevent deadlocks and resource leaks.

This function is registered as a before-shutdown callback during auxiliary process initialization and is automatically invoked when the process is terminating, either normally or due to an error condition.

## Parameters / Member Variables
- : Exit code indicating the reason for shutdown
- : Additional argument data (typically unused, passed as 0)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)

- Called from:
  - Registered as callback via before_shmem_exit() in AuxiliaryProcessMainCommon

## Notes and Other Information
- Declared as static, indicating it's only used within auxprocess.c
- Critical for preventing LWLock deadlocks during error exits
- Much simpler than ShutdownPostgres() since auxiliary processes don't handle transactions
- Automatically invoked by the process exit handler mechanism
- Ensures proper cleanup of condition variable sleep states and statistics reporting
- Essential for maintaining system stability when auxiliary processes terminate unexpectedly