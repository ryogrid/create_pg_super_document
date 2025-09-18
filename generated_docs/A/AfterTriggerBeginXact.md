# AfterTriggerBeginXact

## Location
src/backend/commands/trigger.c: 5073 - 5104

## Overview
Initializes the after-trigger system state at the beginning of a transaction, setting up the necessary data structures for deferred trigger execution.

## Definition


## Detailed Description
AfterTriggerBeginXact is called at transaction start (either explicit BEGIN or implicit for single statements outside transaction blocks) to initialize the after-trigger state structure. This function sets up the firing counter and query depth, and performs assertions to verify that no leftover state exists from previous transactions.

The function initializes the firing_counter to 1 (must not be 0) and sets the query_depth to -1. It also includes several assertions that verify the after-trigger system is in a clean state, checking that various components (state, query_stack, event_cxt, events.head, trans_stack) are NULL and depth counters are zero.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type used for firing_counter)
  - Assert (for state verification)
- Called from:
  - StartTransaction (in src/backend/access/transam/xact.c:2156)

## Notes and Other Information
- The firing_counter is initialized to 1 rather than 0 because 0 has special meaning in the trigger system
- The function includes comprehensive assertions to detect programming errors where the previous transaction didn't clean up properly via AfterTriggerEndXact
- This is part of PostgreSQL's deferred trigger mechanism that allows triggers to be executed at transaction commit time rather than immediately after the triggering event