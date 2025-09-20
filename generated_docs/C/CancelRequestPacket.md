# CancelRequestPacket

## Location
[src/include/libpq/pqcomm.h:134-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqcomm.h#L134-L140)

## Overview
A network protocol structure used to send cancellation requests from PostgreSQL clients to the server, containing the necessary authentication information to safely cancel a running query.

## Definition

```c
typedef struct CancelRequestPacket
{
	/* Note that each field is stored in network byte order! */
	MsgType		cancelRequestCode;	/* code to identify a cancel request */
	uint32		backendPID;		/* PID of client's backend */
	uint32		cancelAuthCode; /* secret key to authorize cancel */
} CancelRequestPacket;
```
## Detailed Description
The CancelRequestPacket structure defines the format for cancel request messages sent over the network from PostgreSQL clients to the server. This packet allows clients to request cancellation of currently executing queries without requiring a full connection teardown. The structure includes authentication information to prevent unauthorized cancellation attempts from other processes.

All fields are stored in network byte order (big-endian) to ensure proper communication across different architectures. The packet uses a specific message type code and requires both the backend process ID and a secret authentication code that was provided during the initial connection establishment.

## Parameters / Member Variables
- `cancelRequestCode`: A message type identifier that marks this packet as a cancellation request
- `backendPID`: The process ID of the backend server process handling the client's connection
- `cancelAuthCode`: A secret authentication token used to verify that the cancel request is legitimate and comes from the authorized client
## Dependencies
- Functions called/Symbols referenced:
  - MsgType
- Called from (representative examples):
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md) (in backend startup handling)
  - [PQcancel](../P/PQcancel.md) (in libpq cancel operations)

## Notes and Other Information
- The authentication mechanism prevents malicious processes from canceling arbitrary queries by requiring knowledge of both the backend PID and the secret cancel code
- Network byte order storage ensures cross-platform compatibility in heterogeneous network environments
- This is part of PostgreSQL's wire protocol for client-server communication
- The cancel request can be sent over a separate connection from the main query connection, allowing cancellation even when the main connection is blocked