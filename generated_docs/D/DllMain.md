# DllMain

## Location
src/bin/pgevent/pgevent.c: 152 - 160

## Overview
DllMain is the optional entry point function for Windows DLLs that handles DLL lifecycle events and initializes the global module handle for the PostgreSQL event logging DLL.

## Definition
```c
BOOL WINAPI DllMain(HANDLE hModule, DWORD ul_reason_for_call, LPVOID lpReserved)
```

## Detailed Description
DllMain is the standard Windows DLL entry point function that is automatically called by the system when the DLL is loaded, unloaded, or when threads are created/destroyed within processes that have loaded the DLL. In the PostgreSQL event logging DLL, this function serves a minimal but essential role: it captures and stores the module handle when the DLL is first loaded into a process.

The function specifically responds to the DLL_PROCESS_ATTACH notification by storing the module handle in the global variable g_module. This handle is later used by DllRegisterServer to determine the DLL's file path via GetModuleFileName, which is necessary for registering the DLL as an event message file in the Windows registry.

## Parameters / Member Variables
- `hModule`: Handle to the DLL module, representing the base address where the DLL is loaded in the process's address space
- `ul_reason_for_call`: Indicates why DllMain is being called (DLL_PROCESS_ATTACH, DLL_PROCESS_DETACH, DLL_THREAD_ATTACH, or DLL_THREAD_DETACH)
- `lpReserved`: Reserved parameter that provides additional context about the DLL loading/unloading process

## Dependencies
- Functions called/Symbols referenced:
  - g_module (global variable assignment)
- Called from (representative examples):
  - Windows system loader during DLL loading
  - LoadLibrary/LoadLibraryEx API calls
  - Automatic loading during process initialization

## Notes and Other Information
- Always returns TRUE indicating successful initialization
- Only processes DLL_PROCESS_ATTACH events, ignoring other lifecycle notifications
- The stored module handle enables path resolution for registry configuration
- Essential for the proper functioning of DllRegisterServer
- Follows the minimal DllMain implementation pattern recommended for most DLLs
- Part of the Windows DLL loading protocol and infrastructure
- Global variable g_module is used by other functions in the same compilation unit