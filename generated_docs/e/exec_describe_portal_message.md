# exec_describe_portal_message

## Location
[src/backend/tcop/postgres.c:2718-2769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2718-L2769)

## Overview
Processes a "Describe" message for a portal, sending the row description of the portal's result set back to the client in the PostgreSQL wire protocol.

## Definition


## Detailed Description
This function handles the Describe message for portals in PostgreSQL's extended query protocol. A portal represents a prepared statement that has been bound with specific parameter values and is ready for execution. The function retrieves the portal by name, validates its existence, and sends back a row description if the portal returns data, or a NoData message if it doesn't. Similar to statement description, it includes safety checks to prevent describing data-returning portals in aborted transaction states to avoid catalog access issues.

## Parameters / Member Variables
- `portal_name`: Name of the portal to describe

## Dependencies
- Functions called/Symbols referenced:
  - [start_xact_command](../s/start_xact_command.md)
  - GetPortalByName
  - PortalIsValid
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [FetchPortalTargetList](../F/FetchPortalTargetList.md)
  - [pq_putemptymessage](../p/pq_putemptymessage.md)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- Portals are the result of binding prepared statements with parameter values
- The function validates portal existence before attempting to describe it
- Special handling for aborted transaction states prevents unsafe catalog access
- Uses the portal's tuple descriptor and format information for row descriptions
- Part of PostgreSQL's extended query protocol implementation alongside statement description