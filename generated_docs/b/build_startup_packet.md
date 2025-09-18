# build_startup_packet

## Location
src/interfaces/libpq/fe-protocol3.c: 2260 - 2277

## Overview
Constructs the actual PostgreSQL startup packet content by serializing connection parameters, user credentials, database information, and environment options into the protocol 3 message format.

## Definition


## Detailed Description
build_startup_packet performs the core work of constructing PostgreSQL startup packets for protocol 3 connections. It uses a two-phase approach: when called with packet=NULL, it calculates the required buffer size; when called with an allocated buffer, it fills the packet with properly formatted connection parameters. The function serializes the protocol version, user credentials, database name, replication settings, client options, application name, encoding settings, and environment-driven GUC parameters into the binary packet format expected by PostgreSQL servers.

The function uses the ADD_STARTUP_OPTION macro to efficiently handle parameter serialization, ensuring proper null-termination and length calculation for each parameter pair.

## Parameters / Member Variables
- : PostgreSQL connection structure containing all connection parameters and settings
- : Target buffer for the startup packet (NULL for size calculation phase)
- : Array of PQEnvironmentOption structures defining environment variable mappings to PostgreSQL parameters

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32 (network byte order conversion)
  - memcpy, strcpy, strlen (string/memory operations)
  - getenv (environment variable access)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - ProtocolVersion (type for protocol version)
  - [PQEnvironmentOption](../P/PQEnvironmentOption.md) (structure type)
- Called from (representative examples):
  - [pqBuildStartupPacket3](../p/pqBuildStartupPacket3.md) (wrapper function in src/interfaces/libpq/fe-protocol3.c)

## Notes and Other Information
- Returns the total length of the constructed packet in bytes
- Uses a dual-phase approach: size calculation followed by buffer filling
- Handles optional parameters gracefully by checking for null/empty values
- Includes protocol version (converted to network byte order) as the first field
- Supports standard connection parameters: user, database, replication, options, application_name, client_encoding
- Processes environment-driven GUC settings through the options parameter
- Terminates the packet with a null byte as required by PostgreSQL protocol
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- Static function used internally within the fe-protocol3.c module