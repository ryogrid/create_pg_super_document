# winsock_strerror

## Location
[src/interfaces/libpq/win32.c:277-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/win32.c#L277-L320)

## Overview
winsock_strerror is a function that provides human-readable error descriptions for Windows socket error codes, using both a static lookup table and dynamic loading of Windows system DLLs as fallback mechanisms.

## Definition

```c
const char *
winsock_strerror(int err, char *strerrbuf, size_t buflen)
```
## Detailed Description
This function implements a two-tier approach to resolving Windows socket error codes into descriptive text messages. It first attempts to find the error code in a static lookup table using LookupWSErrorMessage(). If that fails, it iteratively loads Windows system DLLs (netmsg.dll, winsock.dll, ws2_32.dll, etc.) and uses the Windows FormatMessage() API to retrieve the error description from the system.

The function employs a lazy-loading strategy for the DLLs, loading each library only once when needed and caching the handles for subsequent calls. If all lookup methods fail, it generates a generic "unrecognized socket error" message with the numeric error code. The function always appends the hexadecimal and decimal error code to the end of the message for debugging purposes.

The implementation prioritizes efficiency by checking the static table first (fastest), then progressively trying system DLLs, and finally falling back to a generic message format.

## Parameters / Member Variables
- `err`: The socket error code to look up and describe
- `strerrbuf`: Buffer where the error description string will be written
- `buflen`: Size of the destination buffer to prevent overflow

## Dependencies
- Functions called/Symbols referenced:
  - [LookupWSErrorMessage](../L/LookupWSErrorMessage.md) (internal lookup function)
  - DLLS_SIZE (macro defining size of dlls array)
  - [libpq_gettext](../l/libpq_gettext.md) (localization function)
  - LoadLibraryEx (Windows API)
  - FormatMessage (Windows API)
  - sprintf (standard C library)
  - strlen (standard C library)
- Called from (representative examples):
  - SOCK_STRERROR (macro in libpq-int.h)

## Notes and Other Information
- Returns a pointer to the strerrbuf parameter containing the error description
- Uses FORMAT_MESSAGE_FROM_SYSTEM and FORMAT_MESSAGE_FROM_HMODULE flags for FormatMessage()
- Reserves 64 bytes at the end of the buffer for appending error code information
- Implements lazy loading of Windows system DLLs using LoadLibraryEx with LOAD_LIBRARY_AS_DATAFILE flag
- The dlls array contains handles to various Windows socket-related libraries including netmsg.dll, winsock.dll, ws2_32.dll, wsock32n.dll, mswsock.dll, ws2help.dll, and ws2thk.dll
- Ensures null termination of the result string and prevents buffer overflow
- Uses MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT) to request English error messages
- Part of the Windows-specific implementation in PostgreSQL's libpq client library

## Simplified Source

```c
const char *
winsock_strerror(int err, char *strerrbuf, size_t buflen)
{
    unsigned long flags;
    int offs, i;
    int success = LookupWSErrorMessage(err, strerrbuf);

    // Try loading DLLs if lookup table failed
    for (i = 0; !success && i < DLLS_SIZE; i++) {
        // Load DLL if not already loaded
        if (!dlls[i].loaded) {
            dlls[i].loaded = 1;
            dlls[i].handle = (void *) LoadLibraryEx(dlls[i].dll_name, 0, LOAD_LIBRARY_AS_DATAFILE);
        }

        if (dlls[i].dll_name && !dlls[i].handle)
            continue; // DLL didn't load

        // Set flags for FormatMessage
        flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS
                | (dlls[i].handle ? FORMAT_MESSAGE_FROM_HMODULE : 0);

        // Try to get error message from this DLL
        success = 0 != FormatMessage(flags, dlls[i].handle, err,
                                   MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
                                   strerrbuf, buflen - 64, 0);
    }

    // Generate fallback message if all methods failed
    if (!success) {
        sprintf(strerrbuf, libpq_gettext("unrecognized socket error: 0x%08X/%d"), err, err);
    } else {
        // Append error code to successful message
        strerrbuf[buflen - 1] = '\0';
        offs = strlen(strerrbuf);
        if (offs > (int) buflen - 64)
            offs = buflen - 64;
        sprintf(strerrbuf + offs, " (0x%08X/%d)", err, err);
    }

    return strerrbuf;
}
```