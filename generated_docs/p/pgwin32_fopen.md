# pgwin32_fopen

## Location
src/port/open.c: 195 - 222

## Overview
Provides a POSIX-compatible fopen() function replacement for Windows, converting fopen() mode strings to appropriate file flags.

## Definition
```c
FILE *pgwin32_fopen(const char *fileName, const char *mode)
```

## Detailed Description
This function implements the standard C library fopen() interface on Windows by:

1. **Mode String Parsing**: Converts fopen()-style mode strings ("r", "w", "a", "r+", "w+", "rb", "wb", etc.) into POSIX-style file flags
2. **Flag Translation**: Maps parsed modes to appropriate O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, O_TRUNC, O_APPEND flags
3. **Binary/Text Mode**: Handles 'b' and 't' mode specifiers for binary and text modes
4. **File Descriptor Conversion**: Uses pgwin32_open() to create a file descriptor, then converts it to a FILE* using _fdopen()

The function handles all standard fopen() mode combinations:
- "r", "r+": Read modes
- "w", "w+": Write modes with truncation and creation
- "a": Append mode with creation
- "b": Binary mode flag
- "t": Text mode flag

## Parameters / Member Variables
- `fileName`: Path to the file to open  
- `mode`: Standard fopen()-style mode string (e.g., "r", "wb", "r+", "a")

## Dependencies
- Functions called/Symbols referenced:
  - strstr
  - strchr
  - [pgwin32_open](pgwin32_open.md)
  - _fdopen
- Called from (representative examples):
  - System fopen() calls (through macro redefinition)

## Notes and Other Information
- Returns NULL on failure, following standard fopen() conventions
- Part of PostgreSQL's Windows portability layer that ensures consistent file I/O behavior
- The mode string parsing logic handles overlapping mode specifiers appropriately (e.g., "w+" takes precedence over "w")
- Uses pgwin32_open() internally, which provides the robust Windows file handling with retry logic and error handling
- This function is typically accessed through a macro that redirects standard fopen() calls to pgwin32_fopen()
- Properly handles both the creation flags (O_CREAT, O_TRUNC) and access modes (O_RDONLY, O_WRONLY, O_RDWR) based on the mode string