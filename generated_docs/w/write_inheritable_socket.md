# write_inheritable_socket

## Location
[src/backend/postmaster/launch_backend.c:825-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L825-L845)

## Overview
Duplicates a Windows socket for usage in a child process and stores the socket information in an InheritableSocket structure to work around LSP (Layered Service Provider) issues.

## Definition
```c
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
```

## Detailed Description
This Windows-specific function addresses a common problem with socket inheritance on Windows systems where Layered Service Providers (LSPs) such as antivirus software, firewalls, and download managers interfere with direct socket inheritance. Instead of relying on standard socket inheritance, it uses WSADuplicateSocket() to create socket information that can be safely passed to a child process. The function stores both the original socket and the WSA duplication information in an InheritableSocket structure.

## Parameters / Member Variables
- `dest`: Pointer to an InheritableSocket structure where the socket information will be stored
- `src`: The source socket to be duplicated
- `childpid`: Process ID of the child process that will inherit the socket

## Dependencies
- Functions called/Symbols referenced:
  - WSADuplicateSocket (Windows Socket API)
  - WSAGetLastError (Windows Socket API)
  - ereport
  - [errmsg](../e/errmsg.md)
  - [InheritableSocket](../I/InheritableSocket.md) (structure type)
  - pid_t (type)
  - PGINVALID_SOCKET (constant)
- Called from (representative examples):
  - [save_backend_variables](../s/save_backend_variables.md)

## Notes and Other Information
- This is a Windows-specific function designed to work around LSP compatibility issues
- Only duplicates the socket if it is valid (not 0 and not PGINVALID_SOCKET)  
- Stores the original socket value in dest->origsocket for reference
- Uses WSADuplicateSocket() to create protocol-specific information that can be used to recreate the socket in the child process
- Returns false on failure and logs the specific WSA error code for debugging
- Part of the backend parameter passing mechanism on Windows platforms

## Simplified Source

```c
// Simplified version of write_inheritable_socket
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid) {
    // Store the original socket reference
    dest->origsocket = src;

    // Only duplicate valid sockets (skip null/invalid sockets)
    if (src != 0 && src != PGINVALID_SOCKET) {
        // Use Windows API to duplicate socket for child process
        if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0) {
            // Log error with socket details and return failure
            ereport(LOG, (errmsg("could not duplicate socket %d for use in backend: error code %d",
                                 (int) src, WSAGetLastError())));
            return false;
        }
    }

    return true;
}
```

Key simplifications made:
- Preserved essential Windows socket duplication logic
- Maintained critical error handling and logging
- Added descriptive comments for each logical step
- Kept the WSA API calls as they are core to the function's purpose
- Simplified conditional structure while preserving all original logic paths