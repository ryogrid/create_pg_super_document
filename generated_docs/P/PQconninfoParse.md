# PQconninfoParse

## Location
[src/interfaces/libpq/fe-connect.c:5738-5759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5738-L5759)

## Overview
Public API function that parses a PostgreSQL connection string and returns the resulting connection options array.

## Definition
```c
PQconninfoOption *PQconninfoParse(const char *conninfo, char **errmsg)
```

## Detailed Description
This function serves as the public interface for parsing PostgreSQL connection strings, similar to what PQconnectdb() does internally but without establishing a connection. It takes a connection string in any supported format (URI format like postgresql://user@host/db or keyword=value pairs) and returns a dynamically allocated array of PQconninfoOption structures containing the parsed parameters.

The function is a lightweight wrapper around the internal parse_connection_string() function, providing proper error handling and memory management for public API consumption. It only returns options that are explicitly specified in the input string, not any default values that would normally be applied during connection establishment.

## Parameters / Member Variables
- `conninfo`: Connection string to parse (can be URI format or keyword=value pairs)
- `errmsg`: Pointer to char pointer for error message storage (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - PQExpBufferDataBroken
  - [parse_connection_string](../p/parse_connection_string.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PQExpBufferData](PQExpBufferData.md) (data structure)
  - [PQconninfoOption](PQconninfoOption.md) (data structure)
- Called from (representative examples):
  - [libpqrcv_check_conninfo](../l/libpqrcv_check_conninfo.md)
  - [GetConnection](../G/GetConnection.md)
  - [connectDatabase](../c/connectDatabase.md)
  - [do_connect](../d/do_connect.md)
  - Various PostgreSQL utility programs

## Notes and Other Information
- Returns NULL on failure or out-of-memory conditions
- The returned array must be freed using PQconninfoFree() when no longer needed
- Error messages are malloc'd and should be freed using PQfreemem()
- Does not apply default values - only returns explicitly specified parameters
- Used extensively throughout PostgreSQL utilities for connection string validation and parsing
- Part of the public libpq API, making it available to external applications