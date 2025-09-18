# ExplainIndexScanDetails

## Location
src/backend/commands/explain.c: 3976 - 4011

## Overview
Adds detailed information about IndexScan and IndexOnlyScan operations to EXPLAIN output, including scan direction and index name.

## Definition


## Detailed Description
The  function enhances EXPLAIN output by providing additional context about index scan operations. It shows two key pieces of information that help understand how an index scan is being executed:

1. **Scan Direction**: Whether the index is being scanned forward or backward
2. **Index Name**: The specific index being used for the scan operation

The function handles both text and structured output formats differently:
- **Text format**: Appends details directly to the node description (e.g., "Index Scan using my_index" or "Index Scan Backward using my_index")
- **Structured formats** (JSON/XML/YAML): Provides separate properties for "Scan Direction" and "Index Name"

The scan direction is particularly important for understanding query performance, as backward scans may have different performance characteristics depending on the index implementation and the underlying storage.

## Parameters / Member Variables
- : OID of the index being scanned, used to retrieve the index name
- : ScanDirection enum indicating whether the scan is forward, backward, or undefined
- : ExplainState structure containing output formatting context and destination string buffer

## Dependencies
- Functions called/Symbols referenced:
  - explain_get_index_name
  - ScanDirectionIsBackward
  - appendStringInfoString
  - appendStringInfo
  - quote_identifier
  - ExplainPropertyText
  - BackwardScanDirection
  - ForwardScanDirection
- Called from (representative examples):
  - ExplainNode (for IndexScan nodes)
  - ExplainNode (for IndexOnlyScan nodes)

## Notes and Other Information
- Index names are properly quoted using  to handle names with special characters
- For text format, "Backward" is only displayed when the scan direction is backward (forward is implicit)
- For structured formats, scan direction is always explicitly specified ("Forward", "Backward", or "???")
- The "???" scan direction indicates an unexpected or undefined scan direction value
- This function is specifically designed for IndexScan and IndexOnlyScan node types
- Part of PostgreSQL's comprehensive EXPLAIN infrastructure for understanding query execution plans