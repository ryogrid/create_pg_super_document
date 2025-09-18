# PortalSetResultFormat

## Location
src/backend/tcop/pquery.c: 623 - 685

## Overview
Selects and configures the output format codes for a portal's result columns based on client format requests from Bind message conventions.

## Definition
void PortalSetResultFormat(Portal portal, int nFormats, int16 *formats)

## Detailed Description
PortalSetResultFormat configures how result data will be formatted when returned to the client through a portal. This function must be called after PortalStart for portals that will send results to DestRemote or DestRemoteExecute destinations, though it's not needed for other destination types.

The function handles three scenarios: when individual format codes are specified for each column (nFormats equals the number of columns), when a single format is specified for all columns (nFormats equals 1), and when no formats are specified (using default format 0 for all columns). It allocates memory in the portal's context to store the format array and validates that the number of provided formats matches the number of result columns when individual formats are specified.

## Parameters / Member Variables
- portal: The Portal whose result format is being configured, must have a valid tupDesc
- nFormats: Number of format codes provided in the formats array
- formats: Array of int16 format codes (0 for text, 1 for binary) as per PostgreSQL protocol

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_bind_message](../e/exec_bind_message.md)

## Notes and Other Information
- Must be run after PortalStart and only for portals that return tuples
- Required for DestRemote and DestRemoteExecute destinations, not needed for others
- Format codes follow PostgreSQL wire protocol: 0 for text format, 1 for binary format
- Memory for format array is allocated in the portal's memory context
- Validates format count matches column count when individual formats are specified
- Defaults to text format (0) when no formats are provided
- Located in src/backend/tcop/pquery.c:623-685