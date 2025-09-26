# pg_conn

## Location
src/interfaces/libpq/libpq-int.h: 365 - 650

## Overview
The core structure that stores all state data associated with a single PostgreSQL database connection, encompassing connection parameters, authentication state, I/O buffers, and protocol management.

## Definition


## Detailed Description
The  structure is the central data structure in libpq that represents a single database connection. It maintains all state information necessary for communicating with a PostgreSQL server, including connection parameters, authentication credentials, protocol state, I/O buffers, and error handling.

This structure supports advanced PostgreSQL features including:
- Multi-host connections for high availability and load balancing
- SSL/TLS encryption with comprehensive certificate management
- GSSAPI and SSPI authentication methods
- Pipeline mode for improved performance
- Asynchronous connection and query operations
- COPY operations for bulk data transfer
- Large object operations
- Connection pooling and cancellation support

The structure is designed to handle the complete lifecycle of a database connection from initial parameter parsing through connection establishment, query execution, and eventual cleanup.

## Parameters / Member Variables

### Connection Configuration
- : Host name, IP address, Unix socket path, or comma-separated list for multi-host connections
- : Numeric IP address(es) that take precedence over pghost
- : Port number(s) for database connections
- : Connection establishment timeout in seconds
- : TCP user timeout for network operations

### Authentication and Security
- : Target database name
- : PostgreSQL username
- : Password (if provided directly)
- : Path to file containing passwords
- : SSL connection mode (require, prefer, allow, disable)
- /: Client SSL certificate and key files
- : Whether authentication challenge is required
- : Bitmask of acceptable authentication methods

### Connection State Management
- : Overall connection status (CONNECTION_OK, CONNECTION_BAD, etc.)
- : Status of asynchronous operations
- : Current transaction status
- : Pipeline mode status
- : Whether using non-blocking I/O semantics

### Multi-host Support
- : Number of hosts specified in connection string
- : Index of currently active host
- : Array of pg_conn_host structures with host details
- : IP address of current network connection

### I/O Buffer Management
- /: Input buffer for data from server
- //: Input buffer position tracking
- /: Output buffer for data to server
- : Number of bytes waiting in output buffer

### Protocol Information
- : Socket file descriptor for network connection
- : Frontend/backend protocol version
- : Server version number
- /: Backend process ID and key for cancellation

### Error and Message Handling
- : Expandable buffer for current error messages
- : Temporary buffer for message construction
- : Callback procedures for notice processing

## Dependencies
- Functions called/Symbols referenced:
  - pg_conn_host (for multi-host connection support)
  - PGNoticeHooks (notice message processing)
  - ConnStatusType, PGAsyncStatusType (status enumerations)
  - PQExpBufferData (expandable string buffers)
  - Various SSL, GSS, and SSPI types for security features

- Called from (representative examples):
  - PQconnectdb, PQconnectdbParams (connection establishment functions)
  - PQexec, PQexecParams (query execution functions)
  - All libpq public API functions that operate on connections

## Notes and Other Information
- Located in libpq-int.h:365-650, this is the largest and most central structure in libpq
- The structure is opaque to client applications; all access is through libpq API functions
- Supports both blocking and non-blocking I/O modes for different application architectures
- Multi-host functionality enables automatic failover and load balancing across multiple database servers
- Extensive SSL/TLS support includes certificate verification, SNI, and multiple protocol versions
- Pipeline mode allows multiple queries to be sent before reading results, improving performance
- The structure is designed to be thread-safe when used properly (one connection per thread)
- Memory management is handled internally by libpq, with automatic buffer expansion as needed
- Error messages accumulate in errorMessage buffer and are reported through PGresult objects
- The structure supports PostgreSQL-specific features like LISTEN/NOTIFY, large objects, and COPY operations