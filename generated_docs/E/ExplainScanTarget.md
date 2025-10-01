# ExplainScanTarget

## Location
[src/backend/commands/explain.c:4012-4024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4012-L4024)

## Overview
Shows the target relation (table) of a scan node in EXPLAIN output by delegating to the ExplainTargetRel function.

## Definition

```c
static void
ExplainScanTarget(Scan *plan, ExplainState *es)
```
## Detailed Description
The  function is a simple wrapper that displays the target relation (table or view) being scanned by a Scan node in EXPLAIN output. It extracts the scan relation ID from the Scan plan node and delegates the actual formatting work to the  function.

This function serves as an abstraction layer that:
1. Takes a Scan-specific node structure
2. Extracts the relevant relation information ()
3. Delegates to the more general  function for consistent relation name formatting

The function is used across different types of scan operations (SeqScan, IndexScan, IndexOnlyScan, etc.) to provide consistent table name display in EXPLAIN output. The target relation information typically appears in the node description, showing which table or view is being accessed.

## Parameters / Member Variables
- : Scan node structure containing the plan information and the scan relation ID ()
- : ExplainState structure containing output formatting context and destination string buffer

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainTargetRel](ExplainTargetRel.md)
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md) (for various scan node types including SeqScan, IndexScan, IndexOnlyScan, BitmapHeapScan)

## Notes and Other Information
- This is a thin wrapper function that provides a Scan-specific interface to the general relation explanation functionality
- The actual formatting logic is implemented in , ensuring consistency across different node types
- Used by multiple scan node types to display the target table/view name in EXPLAIN output
- Part of PostgreSQL's modular EXPLAIN infrastructure that separates node-specific interfaces from general formatting functions
- The  field identifies the specific relation being scanned within the query's range table

## Simplified Source

```c
static void
ExplainScanTarget(Scan *plan, ExplainState *es)
{
    // Show the target relation being scanned
    ExplainTargetRel((Plan *) plan, plan->scanrelid, es);
}
```