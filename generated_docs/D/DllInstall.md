# DllInstall

## Location
[src/bin/pgevent/pgevent.c:38-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgevent/pgevent.c#L38-L64)

## Overview
DllInstall is a Windows DLL entry point function that handles command-line installation parameters for the PostgreSQL event logging DLL, providing custom event source registration functionality.

## Definition


## Detailed Description
DllInstall is a standard Windows COM DLL export function that processes command-line arguments during DLL registration. This function is specifically designed for the PostgreSQL event logging system (pgevent.dll) and handles the installation of custom event sources in the Windows Event Log.

The function converts wide character command-line arguments to multibyte strings for the event source name and implements a workaround for the non-standard behavior of regsvr32 when using the /i flag. Due to regsvr32's unusual calling order (DllRegisterServer before DllInstall during installation), the function must handle registration internally when installing to ensure proper event source configuration.

## Parameters / Member Variables
- : Boolean flag indicating whether this is an installation (TRUE) or uninstallation (FALSE) operation
- : Wide character string containing command-line arguments, typically specifying the custom event source name

## Dependencies
- Functions called/Symbols referenced:
  - wcstombs (converts wide character string to multibyte)
  - [DllRegisterServer](DllRegisterServer.md) (registers the DLL and event source)
- Called from (representative examples):
  - Invoked by Windows regsvr32.exe utility with /i flag
  - COM registration framework

## Notes and Other Information
- Requires the /n flag with regsvr32 to prevent premature DllRegisterServer calls that would overwrite custom event source settings
- The global variable  is populated from the command line to specify custom event source names
- Returns S_OK (success) in all cases as this is a simple registration helper
- Part of the Windows Event Log integration system for PostgreSQL logging
- Only relevant on Windows platforms where COM DLL registration is used