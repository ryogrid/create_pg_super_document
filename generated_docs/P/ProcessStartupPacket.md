# ProcessStartupPacket

## Location
src/backend/tcop/backend_startup.c: 453 - 854

## Overview
ProcessStartupPacket reads and processes a client's startup packet, handling protocol negotiation, connection parameters, and special request types like SSL/GSSAPI negotiation or cancel requests.

## Definition


## Detailed Description
ProcessStartupPacket handles the complex process of reading and interpreting PostgreSQL startup packets from clients. It supports multiple packet types including regular startup packets (protocol version 3), SSL/GSSAPI negotiation requests, and cancel requests. The function validates protocol versions, extracts connection parameters (database name, user name, options), handles encryption layer negotiation, and ensures proper packet formatting. It implements security measures against man-in-the-middle attacks and supports both streaming and logical replication connections.

## Parameters / Member Variables
- : Port structure representing the client connection
- : Flag indicating whether SSL negotiation has been completed
- : Flag indicating whether GSSAPI negotiation has been completed

## Dependencies
- Functions called/Symbols referenced:
  - [pq_startmsgread](../p/pq_startmsgread.md)
  - [pq_getbytes](../p/pq_getbytes.md)
  - [pq_endmsgread](../p/pq_endmsgread.md)
  - pg_ntoh32
  - [processCancelRequest](../p/processCancelRequest.md)
  - [secure_write](../s/secure_write.md)
  - [secure_open_server](../s/secure_open_server.md) (SSL builds)
  - [secure_open_gssapi](../s/secure_open_gssapi.md) (GSSAPI builds)
  - [pq_buffer_remaining_data](../p/pq_buffer_remaining_data.md)
  - [parse_bool](../p/parse_bool.md)
  - pg_clean_ascii
  - [SendNegotiateProtocolVersion](../S/SendNegotiateProtocolVersion.md)
  - [pstrdup](../p/pstrdup.md)
  - lappend
- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md)
  - [ProcessStartupPacket](ProcessStartupPacket.md) (recursive for SSL/GSSAPI negotiation)

## Notes and Other Information
- Returns STATUS_OK for successful processing, STATUS_ERROR for failures or special cases
- Supports protocol versions from PG_PROTOCOL_EARLIEST to PG_PROTOCOL_LATEST
- Handles three special packet types: CANCEL_REQUEST_CODE, NEGOTIATE_SSL_CODE, NEGOTIATE_GSS_CODE
- Validates packet length (4 bytes to MAX_STARTUP_PACKET_LENGTH)
- Extracts standard parameters: database, user, options, replication, application_name
- Truncates database and user names to NAMEDATALEN if too long
- Sets backend type to B_WAL_SENDER for replication connections, B_BACKEND otherwise  
- Implements security check for unencrypted data after encryption negotiation
- Supports protocol options beginning with "_pq_." prefix for future extensions
- Database name defaults to user name if not specified
- Located in src/backend/tcop/backend_startup.c:453-854