# HandleAutoVacLauncherInterrupts

## Location
src/backend/postmaster/autovacuum.c: 740 - 774

## Overview
Processes incoming interrupts for the autovacuum launcher process, handling shutdown requests, configuration reloads, and various signal-based events.

## Definition
static void HandleAutoVacLauncherInterrupts(void)

## Detailed Description
This internal function serves as the central interrupt handler for the autovacuum launcher process. It checks for and processes various types of interrupts that can occur during the launcher's operation, including shutdown requests, configuration file changes, barrier events, memory context logging requests, and shared invalidation catchup operations. The function is designed to be called periodically within the launcher's main loop to ensure responsive handling of system events.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [AutoVacLauncherShutdown](../A/AutoVacLauncherShutdown.md)
  - ProcessConfigFile (with PGC_SIGHUP)
  - [AutoVacuumingActive](../A/AutoVacuumingActive.md)
  - [rebuild_database_list](../r/rebuild_database_list.md)
  - [ProcessProcSignalBarrier](../P/ProcessProcSignalBarrier.md)
  - [ProcessLogMemoryContextInterrupt](../P/ProcessLogMemoryContextInterrupt.md)
  - [ProcessCatchupInterrupt](../P/ProcessCatchupInterrupt.md)
- Called from:
  - AutoVacLauncher main loop (line 592 in autovacuum.c)

## Notes and Other Information
- The function processes interrupts in a specific order: shutdown first, then configuration reload, followed by other signal-based events
- During configuration reload, it rebuilds the database list in case autovacuum naptime settings have changed
- If autovacuuming becomes inactive during config reload, it triggers a launcher shutdown
- This is a static function internal to the autovacuum.c module
- The function handles both graceful shutdown scenarios and emergency interrupt processing