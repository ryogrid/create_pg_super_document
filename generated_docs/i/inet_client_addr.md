# inet_client_addr

## Location
src/backend/utils/adt/network.c: 1716 - 1752

## Overview
Returns the IP address of the client connecting to the PostgreSQL server, or NULL if the connection is via Unix socket or if address resolution fails.

## Definition


## Detailed Description
This function provides the IP address of the client that established the current database connection. It's a PostgreSQL built-in function that can be called from SQL to determine the network address of the connecting client.

The function works by:
1. Accessing the current process's port information through MyProcPort
2. Checking if the connection uses a supported address family (AF_INET for IPv4 or AF_INET6 for IPv6)
3. Converting the socket address to a human-readable string format using pg_getnameinfo_all
4. Cleaning IPv6 addresses to remove scope identifiers if present
5. Converting the address string to PostgreSQL's internal network format

The function returns NULL in several cases:
- No port information available (server-side processes without client connections)
- Unix socket connections (local connections not using TCP/IP)
- Unsupported address families
- Address resolution failures

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (structure representing connection port information)
  - MyProcPort (global variable pointing to current process's port)
  - pg_getnameinfo_all (PostgreSQL wrapper for address-to-name conversion)
  - [clean_ipv6_addr](../c/clean_ipv6_addr.md) (function to clean IPv6 address strings)
  - [network_in](../n/network_in.md) (function to parse network address strings)
  - PG_RETURN_INET_P (macro to return inet/cidr data type)
  - PG_RETURN_NULL (macro to return SQL NULL)

- Called from (representative examples):
  - Available as SQL function inet_client_addr() callable from queries

## Notes and Other Information
- This function is accessible from SQL as a built-in function
- Returns NULL for local Unix socket connections, which is important for security checks
- The NI_NUMERICHOST and NI_NUMERICSERV flags ensure numeric IP addresses are returned rather than DNS hostnames
- IPv6 addresses are cleaned to remove zone/scope identifiers that might be present
- Essential for logging, auditing, and access control based on client IP addresses
- Part of PostgreSQL's connection information functions alongside inet_client_port and inet_server_addr