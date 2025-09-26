# getParameterStatus

## Location
[src/interfaces/libpq/fe-protocol3.c:1469-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1469-L1497)

## Overview
Reads and processes a ParameterStatus message from the PostgreSQL server, extracting parameter name-value pairs and storing them in the connection state.

## Definition

```c
structure so it can all be
	 * freed at once.  We don't use NAMEDATALEN because we don't want to tie
	 * this interface to a specific server name length.
	 */
	nmlen = strlen(svname);
```
## Detailed Description
This static function handles ParameterStatus messages ('S') that the server sends to inform the client about runtime parameter values. These messages contain a parameter name followed by its current value. The function reads both the name and value as null-terminated strings and saves them using the pqSaveParameterStatus function, which maintains the parameter status in the connection's parameter status list.

Parameter status messages are used by PostgreSQL to communicate important server configuration values like client_encoding, timezone, server_version, and other runtime parameters that might affect client behavior. This information is accessible to applications through the PQparameterStatus() function.

## Parameters / Member Variables
- : PostgreSQL connection object containing connection state and buffers for reading message data

## Dependencies
- Functions called/Symbols referenced:
  - [pqGets](../p/pqGets.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [pqSaveParameterStatus](../p/pqSaveParameterStatus.md)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md)
  - [getCopyDataMessage](getCopyDataMessage.md)
  - [pqFunctionCall3](../p/pqFunctionCall3.md)

## Notes and Other Information
- Returns 0 on successful message consumption, EOF if insufficient data available
- Function is declared static, limiting its visibility to the fe-protocol3.c file
- Message format: parameter name (null-terminated string), parameter value (null-terminated string)
- Uses a temporary PQExpBuffer for the parameter value to handle potentially large values
- Parameter values are stored in the connection's parameter status hash table
- Entry assumes 'S' message type and length have already been consumed
- Common parameters include: client_encoding, DateStyle, TimeZone, server_version, server_encoding
- Error handling ensures proper cleanup of temporary buffer on read failure