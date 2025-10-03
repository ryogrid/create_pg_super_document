# ExplainIndexScanDetails

## Location
[src/backend/commands/explain.c:3976-4011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3976-L4011)

## Overview
Adds detailed information about IndexScan and IndexOnlyScan operations to EXPLAIN output, including scan direction and index name.

## Definition

```c
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es)
```
## Detailed Description
The  function enhances EXPLAIN output by providing additional context about index scan operations. It shows two key pieces of information that help understand how an index scan is being executed:

1. **Scan Direction**: Whether the index is being scanned forward or backward
2. **Index Name**: The specific index being used for the scan operation

The function handles both text and structured output formats differently:
- **Text format**: Appends details directly to the node description (e.g., "Index Scan using my_index" or "Index Scan Backward using my_index")
- **Structured formats** (JSON/XML/YAML): Provides separate properties for "Scan Direction" and "Index Name"

The scan direction is particularly important for understanding query performance, as backward scans may have different performance characteristics depending on the index implementation and the underlying storage.

## Parameters / Member Variables
- `indexid`: OID of the index being scanned, used to retrieve the index name
- `indexorderdir`: ScanDirection enum indicating whether the scan is forward, backward, or undefined
- `*es`: ExplainState structure containing output formatting context and destination string buffer
## Dependencies
- Functions called/Symbols referenced:
  - [explain_get_index_name](../e/explain_get_index_name.md)
  - ScanDirectionIsBackward
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [ExplainPropertyText](ExplainPropertyText.md)
  - BackwardScanDirection
  - ForwardScanDirection
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md) (for IndexScan nodes)
  - [ExplainNode](ExplainNode.md) (for IndexOnlyScan nodes)

## Notes and Other Information
- Index names are properly quoted using  to handle names with special characters
- For text format, "Backward" is only displayed when the scan direction is backward (forward is implicit)
- For structured formats, scan direction is always explicitly specified ("Forward", "Backward", or "???")
- The "???" scan direction indicates an unexpected or undefined scan direction value
- This function is specifically designed for IndexScan and IndexOnlyScan node types
- Part of PostgreSQL's comprehensive EXPLAIN infrastructure for understanding query execution plans

## Simplified Source

```c
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir, ExplainState *es)
{
    const char *indexname = explain_get_index_name(indexid);

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Text format: append to existing description
        if (ScanDirectionIsBackward(indexorderdir))
            appendStringInfoString(es->str, " Backward");
        appendStringInfo(es->str, " using %s", quote_identifier(indexname));
    } else {
        // Structured format: separate properties
        const char *scandir;
        switch (indexorderdir) {
            case BackwardScanDirection:
                scandir = "Backward";
                break;
            case ForwardScanDirection:
                scandir = "Forward";
                break;
            default:
                scandir = "???";
                break;
        }

        ExplainPropertyText("Scan Direction", scandir, es);
        ExplainPropertyText("Index Name", indexname, es);
    }
}
```