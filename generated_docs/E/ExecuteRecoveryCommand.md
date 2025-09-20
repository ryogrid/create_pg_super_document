# ExecuteRecoveryCommand

## Location
[src/backend/access/transam/xlogarchive.c:295-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L295-L357)

## Overview
Executes external shell commands during PostgreSQL recovery operations, providing a mechanism to run custom scripts like recovery_end_command and archive_cleanup_command.

## Definition

```c
struct the command to be executed
	 */
	xlogRecoveryCmd = replace_percent_placeholders(command, commandName, "r", lastRestartPointFname);
```
## Detailed Description
ExecuteRecoveryCommand is a utility function that safely executes external shell commands during various phases of PostgreSQL recovery. It handles command execution with proper error reporting and signal handling, while providing context about the current recovery state through placeholder substitution.

The function calculates the current archive file cutoff point based on the oldest restart point, which can be used by cleanup commands to determine which archived files are safe to remove. It supports parameterized commands through placeholder replacement, allowing recovery commands to receive information about the current recovery state.

Error handling distinguishes between normal command failures and signal-based termination, with configurable behavior for fatal versus warning-level responses based on the failOnSignal parameter.

## Parameters / Member Variables
- : The shell command string to execute (may contain placeholders)
- : Human-readable name for the command type (used in logging and error messages)  
- : If true, signal-based command termination results in FATAL error; if false, emits WARNING
- : Wait event identifier for statistics reporting during command execution

## Dependencies
- Functions called/Symbols referenced:
  - [GetOldestRestartPoint](../G/GetOldestRestartPoint.md): Retrieves the oldest restart point for cutoff calculations
  - XLByteToSeg: Converts WAL position to segment number
  - [XLogFileName](../X/XLogFileName.md): Generates filename from timeline and segment for placeholder substitution
  - replace_percent_placeholders: Substitutes placeholders in command string with actual values
  - pgstat_report_wait_start/pgstat_report_wait_end: Wait event reporting for statistics
  - system: Executes the constructed shell command
  - wait_result_is_any_signal: Detects if command terminated due to signal
  - wait_result_to_str: Converts wait result to human-readable string
- Called from (representative examples):
  - [CleanupAfterArchiveRecovery](../C/CleanupAfterArchiveRecovery.md): Executes archive_cleanup_command after recovery completion
  - [CreateRestartPoint](../C/CreateRestartPoint.md): Executes cleanup commands during restart point creation

## Notes and Other Information
- Currently used for executing recovery_end_command and archive_cleanup_command
- Supports placeholder substitution (e.g., %r for restart point filename) in command strings
- Provides proper wait event reporting for monitoring command execution duration
- Signal handling follows the same safety principles as RestoreArchivedFile for recovery robustness
- Commands are executed with full shell interpretation, allowing complex scripting
- Failure behavior is configurable - some commands may warrant recovery termination while others should only generate warnings