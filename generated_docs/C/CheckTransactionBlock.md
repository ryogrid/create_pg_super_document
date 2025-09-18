# CheckTransactionBlock

## Location
src/backend/access/transam/xact.c: 3671 - 3714

## Overview
CheckTransactionBlock is the core implementation function that validates whether commands are executing within appropriate transaction contexts, providing either warnings or errors based on configuration.

## Definition


## Detailed Description
This function serves as the underlying implementation for both WarnNoTransactionBlock and RequireTransactionBlock. It performs the actual logic of checking whether a command is executing within a transaction block, subtransaction, or function call context. Based on the throwError parameter, it either issues a warning or raises an error when the command is executed outside of a transaction block.

The function implements a hierarchical check system: it first verifies if the command is within a transaction block, then checks for subtransaction context, and finally validates the top-level execution context. If all these conditions indicate the command is outside an appropriate transaction context, it reports the issue according to the severity level specified by the caller.

## Parameters / Member Variables
- : bool - indicates whether the statement is being executed at the top level (not inside a function)
- : bool - determines whether to raise an ERROR (true) or WARNING (false) when validation fails
- : const char* - name of the statement type for warning/error message formatting

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionBlock](../I/IsTransactionBlock.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - ereport (for error/warning reporting)
- Constants referenced:
  - ERROR, WARNING (severity levels)
  - ERRCODE_NO_ACTIVE_SQL_TRANSACTION
- Called from:
  - [WarnNoTransactionBlock](../W/WarnNoTransactionBlock.md) (with throwError = false)
  - [RequireTransactionBlock](../R/RequireTransactionBlock.md) (with throwError = true)

## Notes and Other Information
This static function is the shared implementation that enables the distinction between warning and error behaviors for transaction block validation. The function design allows for graceful handling of different execution contexts - it silently returns when appropriate contexts are detected (transaction blocks, subtransactions, or function calls), only reporting issues when commands are executed in inappropriate contexts at the top level. The error message format is consistent across all callers, providing clear guidance to users about transaction block requirements.