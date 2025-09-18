# SPI_execute_snapshot

## Location
src/backend/executor/spi.c: 773 - 811

## Overview
SPI_execute_snapshot allows execution of a prepared SPI plan with explicit control over snapshots and trigger behavior, primarily intended for referential integrity (RI) triggers.

## Definition


## Detailed Description
SPI_execute_snapshot is identical to SPI_execute_plan except that it allows the caller to specify exactly which snapshots to use for query execution. The function registers the provided snapshots and gives control over when AFTER triggers are fired. This function is specifically designed for internal use by referential integrity triggers and is not documented in the public SPI documentation.

The function validates the plan and parameters, converts parameters to the internal format, and delegates execution to _SPI_execute_plan with the specified snapshot parameters. Passing InvalidSnapshot for the snapshot parameter will result in normal behavior of fetching a new snapshot for each query.

## Parameters / Member Variables
- `plan`: SPIPlanPtr - The prepared execution plan to execute
- `Values`: Datum * - Array of parameter values for the plan
- `Nulls`: const char * - Array indicating which parameters are NULL
- `snapshot`: Snapshot - The snapshot to use for query execution (InvalidSnapshot for default behavior)
- `crosscheck_snapshot`: Snapshot - Additional snapshot for cross-checking operations
- `read_only`: bool - Whether the execution should be read-only
- `fire_triggers`: bool - Whether AFTER triggers should be fired immediately or queued
- `tcount`: long - Maximum number of tuples to process

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call
  - _SPI_convert_params
  - _SPI_execute_plan
  - _SPI_end_call
  - SPIPlanPtr
  - SPIExecuteOptions
  - _SPI_PLAN_MAGIC
  - SPI_ERROR_ARGUMENT
  - SPI_ERROR_PARAM
- Called from (representative examples):
  - RI_Initial_Check
  - RI_PartitionRemove_Check
  - ri_PerformCheck

## Notes and Other Information
- This function is currently undocumented in spi.sgml as it is intended for internal use by RI triggers only
- Returns standard SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Validates that the plan magic number matches _SPI_PLAN_MAGIC
- Requires that Values parameter is provided when the plan expects arguments (plan->nargs > 0)
- The function handles snapshot registration automatically
- AFTER trigger firing behavior can be controlled to integrate with outer query processing