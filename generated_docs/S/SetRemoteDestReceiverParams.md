# SetRemoteDestReceiverParams

## Location
[src/backend/access/common/printtup.c:100-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L100-L110)

## Overview
The SetRemoteDestReceiverParams function configures a remote destination receiver by associating it with a specific portal for query execution and result transmission.

## Definition
```c
void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)
```

## Detailed Description
This function is used to set up parameters for DestReceiver objects that are configured for remote destinations (DestRemote or DestRemoteExecute). It takes a DestReceiver and associates it with a Portal, which represents a prepared statement or query execution context. The function performs a type cast to DR_printtup and stores the portal reference for later use during query execution. An assertion ensures that the function is only called on receivers configured for remote destinations.

## Parameters / Member Variables
- `self`: DestReceiver pointer that must be a DR_printtup structure configured for remote destinations
- `portal`: Portal object representing the query execution context to be associated with this receiver

## Dependencies
- Functions called/Symbols referenced:
  - Assert (debugging assertion)
  - DR_printtup (printtup receiver structure)
  - DestRemote (remote destination constant)
  - DestRemoteExecute (remote execute destination constant)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_execute_message](../e/exec_execute_message.md)

## Notes and Other Information
- The function includes an assertion to verify the receiver is configured for DestRemote or DestRemoteExecute destinations
- This is a simple parameter-setting function that establishes the connection between a result receiver and its execution context
- The portal parameter provides access to the query plan, parameter values, and other execution state needed for result formatting
- Must be called after printtup_create_DR but before query execution begins

## Simplified Source

```c
// Simplified version of SetRemoteDestReceiverParams
void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal) {
    // Cast the generic receiver to the specific printtup receiver type
    DR_printtup *myState = (DR_printtup *) self;

    // Verify this is a remote destination receiver
    Assert(myState->pub.mydest == DestRemote ||
           myState->pub.mydest == DestRemoteExecute);

    // Associate the portal with this receiver for result processing
    myState->portal = portal;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Maintained the essential type casting and portal assignment
- Preserved the critical assertion for destination type validation
- Clarified the purpose of each operation with inline comments