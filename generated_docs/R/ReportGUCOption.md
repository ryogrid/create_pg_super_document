# ReportGUCOption

## Location
[src/backend/utils/misc/guc.c:2636-2672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2636-L2672)

## Overview
ReportGUCOption transmits a configuration parameter's current value to the frontend client if it has changed since the last report, implementing efficient duplicate detection.

## Definition
static void ReportGUCOption(struct config_generic *record)

## Detailed Description
This static function is responsible for sending individual configuration parameter values to the frontend via PostgreSQL's message protocol. It implements an optimization to avoid sending duplicate reports by comparing the current value with the last reported value stored in the configuration record.

The function operates as follows:
1. Retrieves the current string representation of the parameter value using ShowGUCOption
2. Compares it with the last reported value (if any)
3. If values differ, constructs and sends a ParameterStatus protocol message
4. Updates the last_reported field to track the new value for future comparisons
5. Handles memory management by freeing the old last_reported value and duplicating the new one

The function includes robust memory management with graceful handling of out-of-memory conditions during string duplication.

## Parameters / Member Variables
- : Pointer to config_generic structure containing the configuration parameter to report
  - record->name: The parameter name
  - record->last_reported: Previously reported value for duplicate detection

## Dependencies
- Functions called/Symbols referenced:
  - [ShowGUCOption](../S/ShowGUCOption.md): Converts configuration value to string representation
  - [pq_beginmessage](../p/pq_beginmessage.md): Starts construction of protocol message
  - [pq_sendstring](../p/pq_sendstring.md): Adds string data to protocol message
  - [pq_endmessage](../p/pq_endmessage.md): Finalizes and sends protocol message
  - [guc_free](../g/guc_free.md): Frees GUC-allocated memory
  - [guc_strdup](../g/guc_strdup.md): Duplicates string using GUC memory context
- Called from (representative examples):
  - [BeginReportingGUCOptions](../B/BeginReportingGUCOptions.md): Reports initial values during startup
  - [ReportChangedGUCOptions](ReportChangedGUCOptions.md): Reports changed values during operation

## Notes and Other Information
- Static function, only accessible within guc.c
- Uses PostgreSQL's pq_* protocol functions to construct ParameterStatus messages
- Implements duplicate detection to avoid redundant network traffic
- Handles memory allocation failures gracefully by setting last_reported to NULL
- If guc_strdup() fails due to OOM, may result in duplicate reports in future calls
- Essential component of PostgreSQL's client-server parameter synchronization mechanism
- Uses LOG level for memory allocation in guc_strdup, indicating non-critical nature of tracking