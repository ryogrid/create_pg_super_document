# exec_describe_portal_message

## Location
src/backend/tcop/postgres.c: 2718 - 2769

## Overview
Processes a "Describe" message for a portal, sending the row description of the portal's result set back to the client in the PostgreSQL wire protocol.

## Definition


## Detailed Description
This function handles the Describe message for portals in PostgreSQL's extended query protocol. A portal represents a prepared statement that has been bound with specific parameter values and is ready for execution. The function retrieves the portal by name, validates its existence, and sends back a row description if the portal returns data, or a NoData message if it doesn't. Similar to statement description, it includes safety checks to prevent describing data-returning portals in aborted transaction states to avoid catalog access issues.

## Parameters / Member Variables
- `portal_name`: Name of the portal to describe

## Dependencies
- Functions called/Symbols referenced:
  - start_xact_command
  - GetPortalByName
  - PortalIsValid
  - IsAbortedTransactionBlockState
  - SendRowDescriptionMessage
  - FetchPortalTargetList
  - pq_putemptymessage
- Called from (representative examples):
  - PostgresMain

## Notes and Other Information
- Portals are the result of binding prepared statements with parameter values
- The function validates portal existence before attempting to describe it
- Special handling for aborted transaction states prevents unsafe catalog access
- Uses the portal's tuple descriptor and format information for row descriptions
- Part of PostgreSQL's extended query protocol implementation alongside statement description