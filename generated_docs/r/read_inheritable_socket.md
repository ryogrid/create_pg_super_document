# read_inheritable_socket

## Location
[src/backend/postmaster/launch_backend.c:846-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L846-L882)

## Overview
Reconstructs a socket descriptor from an InheritableSocket structure that was created by write_inheritable_socket, handling the restoration of inherited sockets in child processes.

## Definition
```c
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
```

## Detailed Description
This Windows-specific function is the counterpart to write_inheritable_socket(). It takes an InheritableSocket structure (which contains socket duplication information) and recreates the actual socket descriptor that can be used in the child process. For invalid or null sockets, it simply copies the original socket value. For actual sockets, it uses WSASocket() with the FROM_PROTOCOL_INFO parameters to recreate the socket from the WSA information stored during duplication. The function also closes the original socket to prevent duplicate references.

## Parameters / Member Variables
- `dest`: Pointer to a SOCKET variable where the reconstructed socket descriptor will be stored
- `src`: Pointer to an InheritableSocket structure containing the socket information to restore

## Dependencies
- Functions called/Symbols referenced:
  - WSASocket (Windows Socket API)
  - WSAGetLastError (Windows Socket API)
  - [write_stderr](../w/write_stderr.md)
  - closesocket
  - exit
  - [InheritableSocket](../I/InheritableSocket.md) (structure type)
  - PGINVALID_SOCKET (constant)
  - INVALID_SOCKET (constant)
  - FROM_PROTOCOL_INFO (constant)
- Called from (representative examples):
  - [restore_backend_variables](restore_backend_variables.md)

## Notes and Other Information
- This is a Windows-specific function that complements write_inheritable_socket()
- Handles both valid and invalid socket cases appropriately
- Uses WSASocket() with FROM_PROTOCOL_INFO to reconstruct the socket from protocol information
- Closes the original socket (src->origsocket) after successful reconstruction to prevent duplicate references
- Exits the process with error code 1 if socket creation fails, making this a critical operation
- Part of the backend parameter restoration mechanism on Windows platforms
- Essential for working around Windows LSP (Layered Service Provider) issues with socket inheritance