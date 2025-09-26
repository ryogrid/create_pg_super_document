# SPI_execute_snapshot

## Location
[src/backend/executor/spi.c:773-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L773-L811)

## Overview
SPI_execute_snapshot allows execution of a prepared SPI plan with explicit control over snapshots and trigger behavior, primarily intended for referential integrity (RI) triggers.

## Definition

```c
int
SPI_execute_snapshot(SPIPlanPtr plan,
					 Datum *Values, const char *Nulls,
					 Snapshot snapshot, Snapshot crosscheck_snapshot,
					 bool read_only, bool fire_triggers, long tcount)
```
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
  - [_SPI_begin_call](_SPI_begin_call.md)
  - [_SPI_convert_params](_SPI_convert_params.md)
  - [_SPI_execute_plan](_SPI_execute_plan.md)
  - [_SPI_end_call](_SPI_end_call.md)
  - [SPIPlanPtr](SPIPlanPtr.md)
  - [SPIExecuteOptions](SPIExecuteOptions.md)
  - _SPI_PLAN_MAGIC
  - SPI_ERROR_ARGUMENT
  - SPI_ERROR_PARAM
- Called from (representative examples):
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
  - [ri_PerformCheck](../r/ri_PerformCheck.md)

## Notes and Other Information
- This function is currently undocumented in spi.sgml as it is intended for internal use by RI triggers only
- Returns standard SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Validates that the plan magic number matches _SPI_PLAN_MAGIC
- Requires that Values parameter is provided when the plan expects arguments (plan->nargs > 0)
- The function handles snapshot registration automatically
- AFTER trigger firing behavior can be controlled to integrate with outer query processing