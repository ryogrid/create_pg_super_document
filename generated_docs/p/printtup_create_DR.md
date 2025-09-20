# printtup_create_DR

## Location
[src/backend/access/common/printtup.c:71-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L71-L99)

## Overview
The printtup_create_DR function creates and initializes a DestReceiver for printtup operations, which is responsible for sending query results to the client through PostgreSQL's communication protocol.

## Definition

```c
DestReceiver *
printtup_create_DR(CommandDest dest)
```
## Detailed Description
This function serves as a factory for creating DR_printtup structures, which are specialized DestReceiver objects for handling tuple output operations. It allocates memory for a new DR_printtup structure and initializes all its function pointers and member variables to appropriate values. The function sets up the complete infrastructure needed for sending query results to clients, including the setup of callback functions for startup, shutdown, and destruction operations. A key feature is the automatic determination of whether to send row description messages (T messages) based on the destination type.

## Parameters / Member Variables
- `dest`: CommandDest value specifying the destination for the output (e.g., DestRemote, DestRemoteExecute)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](palloc0.md) (memory allocation)
  - DR_printtup (structure type)
  - [printtup](printtup.md) (tuple output function)
  - [printtup_startup](printtup_startup.md) (startup callback)
  - [printtup_shutdown](printtup_shutdown.md) (shutdown callback)
  - [printtup_destroy](printtup_destroy.md) (destruction callback)
  - DestRemote (destination constant)
  - DestReceiver (base receiver type)
- Called from (representative examples):
  - [CreateDestReceiver](../C/CreateDestReceiver.md)

## Notes and Other Information
- The function automatically sets sendDescrip to true only for DestRemote destinations, but not for DestRemoteExecute
- All attribute-related fields (attrinfo, nattrs, myinfo) are initialized to NULL/0 
- The temporary context and buffer are initialized to NULL and will be set up later during startup
- Uses palloc0 for zero-initialized memory allocation, ensuring all fields start in a known state