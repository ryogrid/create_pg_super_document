# printtup_startup

## Location
[src/backend/access/common/printtup.c:111-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L111-L165)

## Overview
The printtup_startup function initializes the infrastructure needed for sending query results to clients, setting up memory contexts, I/O buffers, and optionally sending row description messages.

## Definition
```c
static void printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
```

## Detailed Description
This function performs the initial setup required before sending query results through a printtup DestReceiver. It creates an I/O buffer for message formatting and establishes a temporary memory context that can be reset between rows to prevent memory leaks from datatype output routines. If the receiver is configured to send row descriptions (sendDescrip flag), it calls SendRowDescriptionMessage to inform the client about the structure of the result set. The function deliberately postpones setting up attribute information until the first tuple is processed, optimizing for cases where no tuples are returned and allowing for dynamic type changes.

## Parameters / Member Variables
- `self`: DestReceiver pointer (cast to DR_printtup internally)
- `operation`: Integer indicating the type of operation being performed
- `typeinfo`: TupleDesc describing the structure of tuples that will be sent

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md) (I/O buffer initialization)
  - AllocSetContextCreate (memory context creation)
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md) (row description transmission)
  - [FetchPortalTargetList](../F/FetchPortalTargetList.md) (portal target list retrieval)
  - ALLOCSET_DEFAULT_SIZES (memory context sizing constant)
- Called from (representative examples):
  - [printtup_create_DR](printtup_create_DR.md) (as rStartup callback)

## Notes and Other Information
- The I/O buffer is created outside the temporary context to allow reuse across multiple rows
- The temporary memory context named "printtup" helps prevent memory leaks from datatype output functions
- Attribute information setup is deferred until the first printtup call for performance optimization
- Row description messages are sent only if sendDescrip flag is true (typically for DestRemote)
- The function supports potential tuple type changes during execution, though this rarely occurs in practice

## Simplified Source

```c
// Simplified version of printtup_startup
static void printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    DR_printtup *myState = (DR_printtup *) self;
    Portal portal = myState->portal;

    // Create I/O buffer for message formatting
    initStringInfo(&myState->buf);

    // Create temporary memory context for row processing
    myState->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
                                               "printtup",
                                               ALLOCSET_DEFAULT_SIZES);

    // Send row description if required
    if (myState->sendDescrip) {
        SendRowDescriptionMessage(&myState->buf,
                                typeinfo,
                                FetchPortalTargetList(portal),
                                portal->formats);
    }

    // Attribute info setup is deferred until first tuple
}
```

Key simplifications made:
- Preserved essential initialization logic
- Maintained I/O buffer and memory context setup
- Kept row description message sending
- Added comment about deferred attribute info setup