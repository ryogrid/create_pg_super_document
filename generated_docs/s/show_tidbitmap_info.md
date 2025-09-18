# show_tidbitmap_info

## Location
src/backend/commands/explain.c: 3592 - 3621

## Overview
Displays statistics about exact and lossy heap blocks processed during EXPLAIN ANALYZE of a BitmapHeapScan node.

## Definition


## Detailed Description
This function formats and outputs heap block statistics for bitmap heap scans when EXPLAIN ANALYZE is used. It displays information about exact and lossy pages that were processed during the scan execution. The function handles different output formats (TEXT vs JSON/XML/YAML) and only shows the information if there are actual pages to report.

For exact pages, the bitmap can identify specific tuples within the page. For lossy pages, the bitmap indicates that the entire page needs to be checked, but doesn't specify which tuples within the page are relevant.

## Parameters / Member Variables
- : BitmapHeapScanState containing execution statistics including exact_pages and lossy_pages counters
- : ExplainState containing output formatting information and the destination string buffer

## Dependencies
- Functions called/Symbols referenced:
  - ExplainPropertyInteger
  - ExplainIndentText
  - appendStringInfoString
  - appendStringInfo
  - appendStringInfoChar
  - EXPLAIN_FORMAT_TEXT
- Called from (representative examples):
  - ExplainNode

## Notes and Other Information
- This function is only called during EXPLAIN ANALYZE, not regular EXPLAIN
- The function only outputs information if at least one exact or lossy page was processed
- Different formatting is applied based on the explain format (structured formats like JSON use property names, while text format uses a more readable format)
- The function is static and only used within the explain.c module