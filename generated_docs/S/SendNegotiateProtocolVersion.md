# SendNegotiateProtocolVersion

## Location
[src/backend/tcop/backend_startup.c:855-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L855-L884)

## Overview
SendNegotiateProtocolVersion sends a protocol negotiation message to inform the client about supported protocol versions and unrecognized protocol options.

## Definition


## Detailed Description
SendNegotiateProtocolVersion constructs and sends a NegotiateProtocolVersion message to the client when they have requested a newer minor protocol version than the server supports or when unrecognized protocol options are encountered. The message informs the client of the highest protocol version the server supports (PG_PROTOCOL_LATEST) and provides a list of any protocol options that were not understood. This allows clients to use optional parameters without fear of connection failure, while ensuring they know which options were accepted.

## Parameters / Member Variables
- : List of protocol option names that were not recognized by the server

## Dependencies
- Functions called/Symbols referenced:
  - pq_beginmessage
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendstring](../p/pq_sendstring.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - list_length
  - lfirst
- Called from (representative examples):
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md)

## Notes and Other Information
- Sends PqMsg_NegotiateProtocolVersion message type to the client
- Message format: protocol version (int32), count of unrecognized options (int32), followed by option names (strings)
- Does not flush the message buffer as it expects other messages to follow
- Allows graceful protocol version negotiation without forcing connection termination  
- Supports both newer protocol version requests and unknown protocol option handling
- Part of the PostgreSQL protocol extension mechanism for backward/forward compatibility
- Located in src/backend/tcop/backend_startup.c:855-884