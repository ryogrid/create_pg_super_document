# DllUnregisterServer

## Location
[src/bin/pgevent/pgevent.c:127-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgevent/pgevent.c#L127-L151)

## Overview
DllUnregisterServer is a standard Windows COM DLL export function that removes the PostgreSQL event logging DLL registration from the Windows Event Log system by deleting the associated registry entries.

## Definition
```c
STDAPI DllUnregisterServer(void)
```

## Detailed Description
DllUnregisterServer implements the standard COM DLL self-unregistration interface for the PostgreSQL event logging system. This function performs the cleanup operation by removing the registry entries that were created by DllRegisterServer. It locates and deletes the event source registry key under the Windows Event Log Application section, effectively unregistering PostgreSQL as an event source from the Windows Event Log system.

The function constructs the same registry key path that was created during registration and removes it entirely, including all subkeys and values such as EventMessageFile and TypesSupported. This ensures a clean uninstallation of the event logging functionality.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - RegDeleteKey (removes registry keys)
  - MessageBox (displays error messages)
  - _snprintf (formats registry key names)
- Called from (representative examples):
  - Windows regsvr32.exe utility with /u flag
  - COM registration framework during uninstallation
  - [DllInstall](DllInstall.md) (indirectly through regsvr32 /u /i)

## Notes and Other Information
- Returns S_OK on success or SELFREG_E_TYPELIB on failure
- Uses the global variable `event_source` to determine which event source to unregister
- Removes the complete registry key: HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\EventLog\Application\[event_source]
- Displays MessageBox error dialogs for user notification on failures
- Counterpart to DllRegisterServer, providing clean uninstallation capability
- Part of the Windows-specific event logging infrastructure cleanup for PostgreSQL
- Essential for proper DLL uninstallation and system cleanup