# ScanState

## Location
src/include/nodes/execnodes.h: 1564 - 1570

## Overview
ScanState is the base structure for executor nodes that perform scans, extending PlanState with scan-specific fields for managing relation access and tuple retrieval.

## Definition


## Detailed Description
ScanState serves as the foundational structure for all scan executor nodes in PostgreSQL. It extends PlanState to provide common functionality for scanning relations or processing tuples from subplans. This structure is used both for physical table scans and for nodes that process output from underlying plan nodes, making it a versatile base for many executor node types.

## Parameters / Member Variables
-   PID TTY          TIME CMD
16034 ?        00:00:00 bash
16061 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node fields
- : Relation being scanned (NULL if scanning output from a subplan rather than a physical relation)
- : Current scan descriptor containing the state and parameters for the table scan (NULL if not scanning a physical relation)
- : Tuple table slot that holds the current scan tuple being processed

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDescData
- Called from (representative examples):
  - ExecScan
  - ExecScanFetch
  - ExecAssignScanProjectionInfo
  - ExecScanReScan

## Notes and Other Information
- Serves as the base type for numerous specific scan node types including SeqScanState, IndexScanState, BitmapHeapScanState, and many others
- The ss_ScanTupleSlot is the primary interface for retrieving tuples, whether from a physical relation or a subplan
- When used for subplan scanning, only ss_ScanTupleSlot is typically meaningful, with the relation fields being NULL
- Provides a unified interface for scan operations across different access methods and data sources
- Essential for implementing the executor's scan-project-join paradigm where scanning is the foundation operation