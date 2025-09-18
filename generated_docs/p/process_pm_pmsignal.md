# process_pm_pmsignal

## Location
src/backend/postmaster/postmaster.c: 3704 - 3861

## Overview
process_pm_pmsignal handles pmsignal conditions representing requests from backends and checks for promote and logrotate requests from pg_ctl in the PostgreSQL postmaster process.

## Definition
static void process_pm_pmsignal(void)

## Detailed Description
process_pm_pmsignal is a comprehensive signal processing function in the postmaster that handles various inter-process communication signals from backend processes. It serves as the central dispatcher for postmaster state transitions and process management operations. The function processes multiple types of signals including recovery state changes, background worker management, autovacuum operations, WAL receiver management, log rotation requests, and promotion signals.

The function implements careful state checking to ensure signals are only processed in appropriate postmaster states, preventing race conditions that could occur when processes start up quickly or exit unexpectedly. It coordinates the startup sequence from recovery through hot standby mode, manages background worker lifecycle, and handles administrative operations like log rotation and database promotion.

## Parameters / Member Variables
- void: Takes no parameters, operates on global postmaster state variables

## Dependencies
- Functions called/Symbols referenced:
  - CheckPostmasterSignal (multiple signal types)
  - ereport (for logging)
  - XLogArchivingAlways (archiving check)
  - [StartChildProcess](../S/StartChildProcess.md) (process spawning)
  - [AddToDataDirLockFile](../A/AddToDataDirLockFile.md) (status reporting)
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md) (worker management)
  - maybe_start_bgworkers (worker startup)
  - [CheckLogrotateSignal](../C/CheckLogrotateSignal.md) (log rotation detection)
  - [signal_child](../s/signal_child.md) (process signaling)
  - [RemoveLogrotateSignalFiles](../R/RemoveLogrotateSignalFiles.md) (cleanup)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md) (autovacuum management)
  - [MaybeStartWalReceiver](../M/MaybeStartWalReceiver.md) (WAL receiver management)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) (state transitions)
  - [CheckPromoteSignal](../C/CheckPromoteSignal.md) (promotion detection)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster event loop)

## Notes and Other Information
- Sets pending_pm_pmsignal to false at the beginning to clear the signal condition
- Includes systemd integration with sd_notify calls for service readiness notification
- Handles multiple recovery states: RECOVERY_STARTED, BEGIN_HOT_STANDBY with careful state validation
- Implements defensive programming against race conditions in process startup/shutdown sequences
- Background worker changes are accepted only when not in stopping state
- Autovacuum launcher can be started even when autovacuuming is disabled as defense against transaction ID wraparound
- The function ordering is important - [PostmasterStateMachine](../P/PostmasterStateMachine.md) is called before CheckPromoteSignal to ensure proper state evaluation