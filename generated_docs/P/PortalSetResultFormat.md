# PortalSetResultFormat

## Location
[src/backend/tcop/pquery.c:623-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L623-L685)

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

## Simplified Source

```c
// Simplified version of PortalSetResultFormat
void PortalSetResultFormat(Portal portal, int nFormats, int16 *formats) {
    // Skip if portal doesn't return tuples
    if (portal->tupDesc == NULL)
        return;

    int natts = portal->tupDesc->natts;

    // Allocate memory for format array in portal context
    portal->formats = (int16 *) MemoryContextAlloc(portal->portalContext,
                                                   natts * sizeof(int16));

    if (nFormats > 1) {
        // Individual format for each column: validate count matches
        if (nFormats != natts)
            ereport(ERROR, "format count mismatch");
        memcpy(portal->formats, formats, natts * sizeof(int16));
    }
    else if (nFormats > 0) {
        // Single format for all columns
        int16 single_format = formats[0];
        for (int i = 0; i < natts; i++)
            portal->formats[i] = single_format;
    }
    else {
        // Default text format (0) for all columns
        for (int i = 0; i < natts; i++)
            portal->formats[i] = 0;
    }
}
```

Key simplifications made:
- Simplified variable declarations and combined where logical
- Abstracted detailed error message to focus on core logic
- Added clear comments explaining the three main scenarios
- Removed platform-specific details while preserving essential algorithm
- Maintained the core branching logic for different format specification cases