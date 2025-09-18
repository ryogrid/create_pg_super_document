# MessageDLL

## Location
[src/interfaces/libpq/win32.c:234-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/win32.c#L234-L267)

## Overview
MessageDLL is a structure that represents a Windows Dynamic Link Library (DLL) entry used for loading system libraries to retrieve socket error messages in the PostgreSQL libpq Windows implementation.

## Definition


## Detailed Description
The MessageDLL structure serves as a registry entry for Windows system DLLs that contain socket error message resources. It is used to implement a fallback mechanism for error message lookup when the static WSErrors table doesn't contain a specific error code. The structure supports lazy loading of DLLs, where each library is loaded only once when needed and the handle is cached for subsequent use.

The structure is instantiated as a static array called 'dlls' that contains entries for various Windows networking-related libraries including netmsg.dll, winsock.dll, ws2_32.dll, wsock32n.dll, mswsock.dll, ws2help.dll, and ws2thk.dll. The last entry in the array serves as a sentinel with a NULL dll_name but with loaded=1, representing the system itself as a message source.

This design allows the winsock_strerror function to progressively search through different Windows system libraries to find localized error messages that may not be available in the compiled-in error table.

## Parameters / Member Variables
- `dll_name`: Pointer to a constant string containing the name of the Windows DLL to load (e.g., "ws2_32.dll")
- `handle`: Void pointer that stores the library handle returned by LoadLibraryEx, initially NULL until the DLL is loaded
- `loaded`: Integer flag (used as boolean) indicating whether an attempt to load this DLL has been made, preventing repeated load attempts

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - DLLS_SIZE (macro that calculates array size)
  - [winsock_strerror](../w/winsock_strerror.md) (uses the dlls array for DLL iteration)

## Notes and Other Information
- The structure is part of the Windows-specific error handling implementation in libpq
- The dlls array is statically initialized with common Windows networking libraries
- Uses lazy loading pattern to minimize resource usage - DLLs are only loaded when needed
- The loaded flag prevents repeated loading attempts for DLLs that fail to load
- The final array entry with NULL dll_name and loaded=1 represents the system message table
- Works in conjunction with the Windows FormatMessage() API to extract error descriptions from system resources
- Part of a two-tier error resolution strategy: static lookup table first, then dynamic DLL loading