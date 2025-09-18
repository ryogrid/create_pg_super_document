# DllRegisterServer

## Location
[src/bin/pgevent/pgevent.c:65-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgevent/pgevent.c#L65-L126)

## Overview
DllRegisterServer is a standard Windows COM DLL export function that registers the PostgreSQL event logging DLL with the Windows Event Log system by creating the necessary registry entries.

## Definition
```c
STDAPI DllRegisterServer(void)
```

## Detailed Description
DllRegisterServer implements the standard COM DLL self-registration interface for the PostgreSQL event logging system. This function creates registry entries under the Windows Event Log Application key to register PostgreSQL as an event source. It performs the complete registration process including setting the DLL path as the event message file and configuring supported event types.

The function retrieves the current DLL's full path and creates a registry key for the event source (defaulting to "PostgreSQL" or custom name from DllInstall). It configures the EventMessageFile registry value to point to the DLL itself, enabling Windows to locate event message resources. The TypesSupported value is set to allow Error, Warning, and Information event types.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetModuleFileName (retrieves DLL path)
  - RegCreateKey (creates registry keys)  
  - RegSetValueEx (sets registry values)
  - RegCloseKey (closes registry handles)
  - MessageBox (displays error messages)
  - _snprintf (formats registry key names)
- Called from (representative examples):
  - [DllInstall](DllInstall.md) (when bInstall is TRUE)
  - Windows regsvr32.exe utility
  - COM registration framework

## Notes and Other Information
- Returns S_OK on success or SELFREG_E_TYPELIB on failure
- Uses the global variable `event_source` to determine the event source name
- Creates registry entries under HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\EventLog\Application
- Sets EventMessageFile to the DLL's own path for message resource lookup
- Configures TypesSupported for ERROR, WARNING, and INFORMATION event types
- Displays MessageBox error dialogs for user notification on failures
- Part of the Windows-specific event logging infrastructure for PostgreSQL