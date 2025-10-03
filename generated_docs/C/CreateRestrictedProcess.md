# CreateRestrictedProcess

## Location
[src/bin/pg_ctl/pg_ctl.c:1756-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1756-L1895)

## Overview
Creates a Windows process with restricted security privileges by using a restricted token and optional job object sandbox to enhance security when running PostgreSQL processes.

## Definition

```c
static int
CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, bool as_service)
```
## Detailed Description
This function implements a comprehensive security sandboxing mechanism for Windows processes. It creates a restricted token by removing administrative and power user privileges, drops dangerous privileges, and optionally creates a job object to further constrain the process. The function first obtains the current process token, then creates a restricted version by removing specific SIDs (Administrators and Power Users groups) and privileges returned by GetPrivilegesToDelete(). If job objects are supported and the process isn't already in one, it creates a job with various restrictions including UI limitations and security constraints. The process is created in a suspended state, assigned to the job object, and then resumed.

## Parameters / Member Variables
- `*cmd`: Command line string for the process to be created
- `*processInfo`: Pointer to PROCESS_INFORMATION structure that will receive information about the created process
- `as_service`: Boolean indicating whether the process is being created as a service (affects job object behavior)
## Dependencies
- Functions called/Symbols referenced:
  - [InheritStdHandles](../I/InheritStdHandles.md)
  - OpenProcessToken (Windows API)
  - AllocateAndInitializeSid (Windows API)
  - [GetPrivilegesToDelete](../G/GetPrivilegesToDelete.md)
  - CreateRestrictedToken (Windows API)
  - [AddUserToTokenDacl](../A/AddUserToTokenDacl.md)
  - CreateProcessAsUser (Windows API)
  - IsProcessInJob (Windows API)
  - CreateJobObject (Windows API)
  - SetInformationJobObject (Windows API)
  - AssignProcessToJobObject (Windows API)
  - ResumeThread (Windows API)
  - [write_stderr](../w/write_stderr.md)
- Called from (representative examples):
  - [start_postmaster](../s/start_postmaster.md)
  - [pgwin32_ServiceMain](../p/pgwin32_ServiceMain.md)
  - [get_restricted_token](../g/get_restricted_token.md)
  - [spawn_process](../s/spawn_process.md)

## Notes and Other Information
- Returns 0 on success, non-zero on failure (same convention as CreateProcess)
- Job objects only work reliably when running as a service because they're automatically destroyed when pg_ctl exits
- The function removes Administrator and Power User group memberships to reduce privileges
- UI restrictions prevent the process from interacting with the desktop, clipboard, and system settings
- The job object handle is intentionally not closed to keep the restrictions active until pg_ctl shuts down
- Error handling includes detailed Windows error codes for debugging
- This is a critical security feature that helps prevent privilege escalation attacks