# InheritableSocket

## Location
[src/backend/postmaster/launch_backend.c:87-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L87-L89)

## Overview
InheritableSocket is a platform-specific type definition that enables socket inheritance between parent and child processes in PostgreSQL. It abstracts the complexities of socket inheritance across Windows and Unix-like systems.

## Definition


## Detailed Description
InheritableSocket provides a cross-platform abstraction for socket inheritance in PostgreSQL's process launch mechanism. On Windows, it wraps a complex structure containing the original socket handle and WSA protocol information needed for socket duplication across process boundaries. This is necessary because Windows requires special handling through WSADuplicateSocket() due to interference from Layered Service Providers (LSPs) like antivirus software and firewalls that break direct socket inheritance.

On Unix-like systems, socket inheritance works through standard file descriptor inheritance, so InheritableSocket is simply aliased to a plain integer file descriptor.

## Parameters / Member Variables
### Windows (struct members):
- : The original SOCKET handle value, or PGINVALID_SOCKET if not representing an actual socket
- : WSAPROTOCOL_INFO structure containing the protocol information needed to recreate the socket in the child process

### Unix (typedef):
- Simple integer representing the socket file descriptor

## Dependencies
- Functions called/Symbols referenced:
  - SOCKET (Windows socket type)
  - WSAPROTOCOL_INFO (Windows structure)
  - PGINVALID_SOCKET (PostgreSQL constant)
- Called from (representative examples):
  - [write_inheritable_socket](../w/write_inheritable_socket.md)
  - [read_inheritable_socket](../r/read_inheritable_socket.md)

## Notes and Other Information
- Critical for PostgreSQL's process forking mechanism on Windows where socket inheritance is problematic
- Part of the backend launch infrastructure in src/backend/postmaster/launch_backend.c
- Used within BackendParameters structure to pass socket information to child processes
- The Windows implementation addresses LSP interference that commonly breaks socket inheritance
- On Unix systems, the simpler typedef leverages the operating system's native file descriptor inheritance