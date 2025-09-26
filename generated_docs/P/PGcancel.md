# PGcancel

## Location
src/interfaces/libpq/libpq-fe.h: 204 - 211

## Overview
PGcancel encapsulates the information needed to cancel a running query on an existing PostgreSQL connection. It stores the network and authentication details required to send a cancellation request to a specific backend process.

## Definition


The actual structure definition is in  as .

## Detailed Description
PGcancel represents a lightweight cancellation token that can be used to cancel long-running queries on PostgreSQL backends. Unlike PGcancelConn which maintains a full connection, PGcancel stores only the essential information needed to send a cancel request: the target backend's address, process ID, and cancellation key.

This approach is part of PostgreSQL's traditional cancellation API, designed to be extracted from an active PGconn and used independently. The cancellation mechanism works by sending a special cancel request message to the backend using the stored connection parameters.

Key characteristics:
- Contains minimal information needed for cancellation
- Can be created from an existing PGconn connection  
- Supports TCP keepalive configuration for reliable delivery
- Used with the older synchronous PQcancel API
- Complements the newer PGcancelConn asynchronous cancellation approach

The cancel request uses the backend's process ID and a secret cancellation key to ensure that only authorized clients can cancel queries.

## Parameters / Member Variables
- **raddr**:  - Remote address of the PostgreSQL server to send the cancel request to
- **be_pid**:  - Process ID of the backend that should be canceled
- **be_key**:  - Secret cancellation key for the target backend process
- **pgtcp_user_timeout**:  - TCP user timeout setting for the cancel connection
- **keepalives**:  - Flag indicating whether to use TCP keepalives (boolean)
- **keepalives_idle**:  - Time between TCP keepalive probes
- **keepalives_interval**:  - Time between TCP keepalive retransmissions  
- **keepalives_count**:  - Maximum number of TCP keepalive retransmissions

## Dependencies
- Functions called/Symbols referenced:
  - pg_cancel (the underlying struct type)
  - SockAddr (network address structure)
- Called from (representative examples):
  - PQgetCancel - Extract cancel information from PGconn
  - PQcancel - Send synchronous cancellation request
  - PQfreeCancel - Free PGcancel resources
  - SetCancelConn - Utilities for cancel connection management
  - PQrequestCancel - Legacy cancellation function

## Notes and Other Information
- The structure contents are intentionally opaque to applications
- Created via PQgetCancel() from an existing PGconn connection
- Must be freed with PQfreeCancel() when no longer needed
- Represents the traditional synchronous cancellation approach
- The be_pid and be_key provide security - only the original connection holder can cancel
- Network timeouts and keepalives ensure reliable delivery of cancel requests
- Part of the original libpq cancellation API, now complemented by PGcancelConn for asynchronous operations
- Can be used safely from signal handlers (unlike PGcancelConn)
- Thread-safe once created since it contains only read-only data