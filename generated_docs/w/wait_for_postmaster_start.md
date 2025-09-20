# wait_for_postmaster_start

## Location
[src/bin/pg_ctl/pg_ctl.c:592-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L592-L708)

## Overview
Waits for the PostgreSQL postmaster to complete its startup process and become ready to accept connections, monitoring the postmaster.pid file and process status.

## Definition

```c
static WaitPMResult
wait_for_postmaster_start(pid_t pm_pid, bool do_checkpoint)
```
## Detailed Description
The  function implements a polling mechanism to monitor postmaster startup progress. It repeatedly checks the postmaster.pid lock file to determine when the server has successfully started and is ready to accept connections.

**Key behaviors:**
- Polls the postmaster.pid file up to  times
- Validates the PID and start time in the lock file to ensure it's monitoring the correct postmaster instance
- Checks the status line for PM_STATUS_READY or PM_STATUS_STANDBY to confirm readiness
- Monitors process liveness to detect early failures
- Provides progress feedback by printing dots (except on Windows service mode)
- Handles platform differences between Unix and Windows process monitoring

**Return values:**
- : Server started successfully and is ready
- : Server process died during startup  
- : Timeout exceeded, server still starting

The function includes safeguards against monitoring stale PID files by comparing start times and process IDs, with some tolerance for clock skew between processes.

## Parameters / Member Variables
- : The process ID of the postmaster (or shell process on Windows) to monitor
- : Boolean flag enabling Windows service control manager checkpoints during the wait

## Dependencies
- Functions called/Symbols referenced:
  -  - Reads the postmaster.pid lock file
  -  - Frees memory allocated by readfile
  -  (Unix) - Checks if child process is still alive
  -  (Windows) - Checks if process handle is still valid
  -  - Prints progress dots
  -  - Cross-platform sleep function
  -  (Windows) - Updates service control manager
- Called from (representative examples):
  -  in pg_ctl.c
  -  (Windows service mode)

## Notes and Other Information
- The function assumes PostgreSQL v10 or later due to reliance on the PM_STATUS line in postmaster.pid
- On Windows, the PID validation is relaxed since the monitored PID may be a shell ancestor process
- Windows service mode updates the service control manager checkpoint to prevent timeout termination
- The polling interval is configurable via WAITS_PER_SEC (typically 10 checks per second)
- Clock skew tolerance of 2 seconds is built in for cross-process time comparisons
- Progress indication helps users understand that startup is proceeding normally during lengthy initialization