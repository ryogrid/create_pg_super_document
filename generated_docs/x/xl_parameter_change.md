# xl_parameter_change

## Location
src/include/access/xlog_internal.h: 273 - 283

## Overview
A data structure that logs changes in PostgreSQL configuration parameters that are important for Hot Standby functionality, ensuring standby servers can maintain consistency with parameter changes on the primary server.

## Definition


## Detailed Description
xl_parameter_change is a WAL record structure used to log changes in critical PostgreSQL configuration parameters that affect Hot Standby operations. When these parameters are modified on the primary server, the changes must be communicated to standby servers to ensure they can properly maintain consistency and avoid conflicts.

This structure is written to the WAL when parameter changes are detected, using the XLOG_PARAMETER_CHANGE record type (0x60). The logged information allows standby servers to adjust their behavior according to the new parameter values, preventing issues that could arise from parameter mismatches between primary and standby servers.

## Parameters / Member Variables
- : Maximum number of concurrent connections allowed
- : Maximum number of background worker processes
- : Maximum number of WAL sender processes for replication
- : Maximum number of prepared transactions that can exist simultaneously
- : Maximum number of locks that can be held by a single transaction
- : Level of WAL logging (minimal, replica, logical)
- : Whether to log hint bits changes in WAL
- : Whether to track commit timestamps for transactions

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - primitive types only)
- Called from (representative examples):
  - [XLogReportParameters](../X/XLogReportParameters.md) (writes parameter changes to WAL)
  - [xlog_redo](xlog_redo.md) (processes parameter change records during recovery)
  - [xlog_desc](xlog_desc.md) (describes parameter change records for debugging)
  - [SummarizeXlogRecord](../S/SummarizeXlogRecord.md) (summarizes parameter changes in WAL)
  - [xlog_decode](xlog_decode.md) (decodes parameter changes for logical replication)

## Notes and Other Information
- Associated with WAL record type XLOG_PARAMETER_CHANGE (0x60)
- Critical for Hot Standby functionality and replication consistency
- Parameters logged are those that can affect standby server behavior
- Changes are detected and logged by XLogReportParameters function
- Part of PostgreSQL's mechanism to ensure configuration consistency across primary and standby servers
- Used during recovery to apply parameter changes on standby servers