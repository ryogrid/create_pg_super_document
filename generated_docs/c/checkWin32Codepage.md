# checkWin32Codepage

## Location
[src/bin/psql/command.c:4015-4039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4015-L4039)

## Overview
A Windows-specific utility function that checks for code page mismatches between the Windows system console and the active Windows code page, warning users about potential character encoding issues.

## Definition


## Detailed Description
This function is designed specifically for Windows environments to detect and warn about code page inconsistencies that could cause display problems with 8-bit characters in psql. It compares the Active Code Page (ACP) used by Windows applications with the Console Code Page (CP) used by the console window. When these differ, it can result in incorrect character rendering, particularly for non-ASCII characters.

The function uses Windows API calls GetACP() and GetConsoleCP() to retrieve the respective code pages and displays a localized warning message if they don't match, directing users to the psql reference documentation for guidance on resolving Windows-specific character encoding issues.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetACP() (Windows API)
  - GetConsoleCP() (Windows API)
  - printf()
  - _() (localization macro)
- Called from:
  - [connection_warnings](connection_warnings.md) (at src/bin/psql/command.c:3957)

## Notes and Other Information
- This function is conditionally compiled only on Windows platforms (enclosed in #ifdef WIN32)
- It is called during psql startup (when in_startup is true) as part of the connection warnings display
- The warning message references the 'Notes for Windows users' section in the psql documentation
- This addresses a common issue on Windows where the console and system code pages can differ, leading to character display problems
- The function is static, meaning it's only accessible within the command.c source file