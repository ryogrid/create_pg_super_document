# exec_describe_portal_message

## Location
[src/backend/tcop/postgres.c:2718-2769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2718-L2769)

## Overview
Processes a "Describe" message for a portal, sending the row description of the portal's result set back to the client in the PostgreSQL wire protocol.

## Definition

```c
static void
exec_describe_portal_message(const char *portal_name)
```
## Detailed Description
This function handles the Describe message for portals in PostgreSQL's extended query protocol. A portal represents a prepared statement that has been bound with specific parameter values and is ready for execution. The function retrieves the portal by name, validates its existence, and sends back a row description if the portal returns data, or a NoData message if it doesn't. Similar to statement description, it includes safety checks to prevent describing data-returning portals in aborted transaction states to avoid catalog access issues.

## Parameters / Member Variables
- `portal_name`: Name of the portal to describe

## Dependencies
- Functions called/Symbols referenced:
  - [start_xact_command](../s/start_xact_command.md)
  - [GetPortalByName](../G/GetPortalByName.md)
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

## Simplified Source

```c
// Simplified version of exec_describe_portal_message
static void exec_describe_portal_message(const char *portal_name) {
    Portal portal;

    // Step 1: Start transaction and switch to proper memory context
    start_xact_command();
    MemoryContextSwitchTo(MessageContext);

    // Step 2: Lookup and validate the portal
    portal = GetPortalByName(portal_name);
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
                       errmsg("portal \"%s\" does not exist", portal_name)));
    }

    // Step 3: Check transaction state for data-returning portals
    if (IsAbortedTransactionBlockState() && portal->tupDesc) {
        ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                       errmsg("current transaction is aborted, "
                             "commands ignored until end of transaction block")));
    }

    // Step 4: Early return if not sending to remote client
    if (whereToSendOutput != DestRemote)
        return;

    // Step 5: Send appropriate response based on portal type
    if (portal->tupDesc) {
        // Portal returns data - send row description
        SendRowDescriptionMessage(&row_description_buf,
                                portal->tupDesc,
                                FetchPortalTargetList(portal),
                                portal->formats);
    } else {
        // Portal doesn't return data - send NoData message
        pq_putemptymessage(PqMsg_NoData);
    }
}
```

Key simplifications made:
- Removed detailed comments for brevity while preserving essential logic flow
- Consolidated the main execution steps into clearly labeled sections
- Simplified error handling descriptions while maintaining the essential checks
- Focused on the main execution path and core functionality
- Preserved all critical validation and safety checks