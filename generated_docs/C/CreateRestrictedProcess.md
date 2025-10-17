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

## Simplified Source

```c
static int
CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, bool as_service)
{
    STARTUPINFO si;
    HANDLE origToken, restrictedToken;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    SID_AND_ATTRIBUTES dropSids[2];
    PTOKEN_PRIVILEGES delPrivs;
    BOOL inJob;
    int r;

    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    InheritStdHandles(&si);

    // Get current process token
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken)) {
        write_stderr("could not open process token: error code %lu", GetLastError());
        return 0;
    }

    // Create SIDs for Administrator and Power Users groups to remove
    ZeroMemory(&dropSids, sizeof(dropSids));
    if (!AllocateAndInitializeSid(&NtAuthority, 2,
                                  SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS,
                                  0, 0, 0, 0, 0, 0, &dropSids[0].Sid) ||
        !AllocateAndInitializeSid(&NtAuthority, 2,
                                  SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS,
                                  0, 0, 0, 0, 0, 0, &dropSids[1].Sid)) {
        write_stderr("could not allocate SIDs: error code %lu", GetLastError());
        return 0;
    }

    // Get privileges to remove
    delPrivs = GetPrivilegesToDelete(origToken);
    if (delPrivs == NULL)
        return 0;

    // Create restricted token by dropping SIDs and privileges
    if (!CreateRestrictedToken(origToken, 0,
                               sizeof(dropSids) / sizeof(dropSids[0]), dropSids,
                               delPrivs->PrivilegeCount, delPrivs->Privileges,
                               0, NULL, &restrictedToken)) {
        write_stderr("could not create restricted token: error code %lu", GetLastError());
        // Cleanup and return
        free(delPrivs);
        FreeSid(dropSids[1].Sid);
        FreeSid(dropSids[0].Sid);
        CloseHandle(origToken);
        return 0;
    }

    // Cleanup intermediate objects
    free(delPrivs);
    FreeSid(dropSids[1].Sid);
    FreeSid(dropSids[0].Sid);
    CloseHandle(origToken);

    // Create process with restricted token
    AddUserToTokenDacl(restrictedToken);
    r = CreateProcessAsUser(restrictedToken, NULL, cmd, NULL, NULL, TRUE,
                           CREATE_SUSPENDED, NULL, NULL, &si, processInfo);

    // Create job object for additional security restrictions
    if (IsProcessInJob(processInfo->hProcess, NULL, &inJob) && !inJob) {
        HANDLE job;
        char jobname[128];

        sprintf(jobname, "PostgreSQL_%lu", (unsigned long) processInfo->dwProcessId);
        job = CreateJobObject(NULL, jobname);

        if (job) {
            // Set basic limits and UI restrictions
            JOBOBJECT_BASIC_LIMIT_INFORMATION basicLimit;
            JOBOBJECT_BASIC_UI_RESTRICTIONS uiRestrictions;
            JOBOBJECT_SECURITY_LIMIT_INFORMATION securityLimit;

            ZeroMemory(&basicLimit, sizeof(basicLimit));
            ZeroMemory(&uiRestrictions, sizeof(uiRestrictions));
            ZeroMemory(&securityLimit, sizeof(securityLimit));

            basicLimit.LimitFlags = JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION |
                                   JOB_OBJECT_LIMIT_PRIORITY_CLASS;
            basicLimit.PriorityClass = NORMAL_PRIORITY_CLASS;
            SetInformationJobObject(job, JobObjectBasicLimitInformation, &basicLimit, sizeof(basicLimit));

            uiRestrictions.UIRestrictionsClass = JOB_OBJECT_UILIMIT_DESKTOP |
                                               JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
                                               JOB_OBJECT_UILIMIT_EXITWINDOWS |
                                               JOB_OBJECT_UILIMIT_READCLIPBOARD |
                                               JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS |
                                               JOB_OBJECT_UILIMIT_WRITECLIPBOARD;
            SetInformationJobObject(job, JobObjectBasicUIRestrictions, &uiRestrictions, sizeof(uiRestrictions));

            securityLimit.SecurityLimitFlags = JOB_OBJECT_SECURITY_NO_ADMIN |
                                              JOB_OBJECT_SECURITY_ONLY_TOKEN;
            securityLimit.JobToken = restrictedToken;
            SetInformationJobObject(job, JobObjectSecurityLimitInformation, &securityLimit, sizeof(securityLimit));

            AssignProcessToJobObject(job, processInfo->hProcess);
            // Intentionally don't close job handle to keep restrictions active
        }
    }

    CloseHandle(restrictedToken);
    ResumeThread(processInfo->hThread);

    return r;
}
```