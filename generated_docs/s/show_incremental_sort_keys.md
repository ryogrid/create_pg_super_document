# show_incremental_sort_keys

## Location
[src/backend/commands/explain.c:2574-2590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2574-L2590)

## Overview
Displays the sort keys for an IncrementalSort node during query execution plan explanation.

## Definition


## Detailed Description
This function is responsible for showing the sort keys used by an IncrementalSort plan node during EXPLAIN command output. IncrementalSort is an optimization that builds upon existing sorted data by performing additional sorting only on the remaining columns. The function extracts the sort key information from the IncrementalSort plan and delegates to  to format and display the sorting information, including both presorted columns and additional sort columns.

## Parameters / Member Variables
- : Pointer to the IncrementalSortState containing the runtime state and plan information for the incremental sort operation
- : List of ancestor plan nodes in the execution tree, used for context in the explanation output
- : ExplainState containing formatting options and output settings for the EXPLAIN command

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that formats and displays sort key information
  - : Plan node structure containing sort configuration
  - : Runtime state structure for incremental sort operations
  - : State structure for EXPLAIN command formatting
- Called from (representative examples):
  - : Main function that handles explanation of different plan node types (at line 2228)

## Notes and Other Information
- This function is part of PostgreSQL's EXPLAIN command infrastructure located in src/backend/commands/explain.c:2574-2590
- It specifically handles the T_IncrementalSort case in the ExplainNode function
- The function accesses both  (total sort columns) and  (already sorted columns) to show the incremental nature of the sort
- The sorting information includes column indexes, operators, collations, and null handling preferences
- This is a static function, only accessible within the explain.c compilation unit