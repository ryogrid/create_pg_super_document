# MaybeStartWalSummarizer

## Location
[src/backend/postmaster/postmaster.c:4072-4090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4072-L4090)

## Overview
MaybeStartWalSummarizer conditionally starts a WAL summarizer process when WAL summarization is enabled, no summarizer is running, and the postmaster is in normal operation or hot standby mode.

## Definition

```c
static void
MaybeStartWalSummarizer(void)
```
## Detailed Description
MaybeStartWalSummarizer implements conditional startup logic for the WAL summarizer process, which is responsible for creating WAL summaries to enable incremental backup functionality. The function starts a summarizer only when all required conditions are met: the summarize_wal configuration parameter is enabled, no summarizer is currently running (WalSummarizerPID == 0), the postmaster is in either normal running state (PM_RUN) or hot standby mode (PM_HOT_STANDBY), and the system is not in immediate shutdown mode.

Unlike WAL receivers which can start during various recovery states, WAL summarizers only start during normal database operations since they need the database to be fully functional to create meaningful summaries. The function is designed to be called repeatedly and handles its own state checking.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [StartChildProcess](../S/StartChildProcess.md) (creates the WAL summarizer process with B_WAL_SUMMARIZER type)
- [Variables](../V/Variables.md) referenced:
  - summarize_wal (configuration parameter enabling WAL summarization)
  - WalSummarizerPID (tracks current summarizer process ID)
  - pmState (postmaster state - checked against PM_RUN and PM_HOT_STANDBY)
  - Shutdown (shutdown state - compared with SmartShutdown)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop for regular checks)
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (restart after child process termination)

## Notes and Other Information
- WAL summarization supports incremental backup functionality by creating summary information
- Only operates during normal database states (PM_RUN, PM_HOT_STANDBY), not during startup/recovery
- Controlled by the summarize_wal GUC parameter (PGC_SIGHUP level)
- Respects shutdown modes - won't start during immediate shutdown
- Simpler startup logic compared to WAL receivers since it doesn't need complex race condition handling
- The summarizer process runs continuously once started, until shutdown or process termination