# internal_forkexec

## Location
[src/backend/postmaster/launch_backend.c:294-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L294-L403)

## Overview
Creates a new PostgreSQL child process using fork+exec (Unix) or CreateProcess (Windows) in EXEC_BACKEND mode, with parameter passing through temporary files or shared memory.

## Definition
```c
static pid_t internal_forkexec(const char *child_kind, 
                              char *startup_data, size_t startup_data_len, 
                              ClientSocket *client_sock)
```

## Detailed Description
internal_forkexec implements the EXEC_BACKEND mechanism for spawning PostgreSQL child processes. This approach is mandatory on Windows and optional on Unix systems for testing. Unlike simple fork(), this method creates a completely new process that does not inherit the parent 's memory state, requiring explicit parameter passing and state restoration.

On Unix systems, it writes backend parameters to a temporary file, then fork+exec a new postgres process with `--forkchild=<child_kind>` and the parameter file path. On Windows, it uses CreateProcess() with shared memory for parameter passing.

The child process will start execution in SubPostmasterMain() which reads the parameters and restores the necessary state before calling the appropriate child main function.

## Parameters / Member Variables
- `child_kind`: String name of the child process type (e.g., "backend", "checkpointer", "bgwriter")
- `startup_data`: Optional initialization data specific to the child process type
- `startup_data_len`: Size of the startup_data buffer
- `client_sock`: Optional client socket information for backend processes

## Dependencies
- Functions called/Symbols referenced:
  - [save_backend_variables](../s/save_backend_variables.md) (to serialize state)
  - SizeOfBackendParameters (for memory allocation)
  - [fork_process](../f/fork_process.md) (Unix: for forking)
  - execv (Unix: to execute new process)
  - CreateProcess (Windows: to create new process)
  - [AllocateFile](../A/AllocateFile.md)/FreeFile (Unix: for temp file I/O)
  - CreateFileMapping/MapViewOfFile (Windows: for shared memory)
- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (when EXEC_BACKEND is defined)

## Notes and Other Information
- This is a static function only available when EXEC_BACKEND is compiled in
- Has separate implementations for Unix (file-based parameter passing) and Windows (shared memory-based)
- On Unix, creates temporary files in PG_TEMP_FILES_DIR with unique names to avoid conflicts
- On Windows, creates a suspended process initially, then resumes after parameter setup
- Returns -1 on failure with appropriate error logging, or the child PID on success
- The child process must be able to read and interpret the parameters file/memory to restore state
- Located in src/backend/postmaster/launch_backend.c:294-403 (Unix) and similar range for Windows
- Critical for platforms where fork() is not available or reliable (primarily Windows)

## Simplified Source

```c
// Simplified version of internal_forkexec (Unix implementation)
static pid_t internal_forkexec(const char *child_kind, char *startup_data,
                              size_t startup_data_len, ClientSocket *client_sock) {
    pid_t pid;
    char tmpfilename[MAXPGPATH];
    BackendParameters *param;
    FILE *fp;
    char *argv[4];
    char forkav[MAXPGPATH];

    // Step 1: Prepare backend parameters for serialization
    size_t paramsz = SizeOfBackendParameters(startup_data_len);
    param = palloc0(paramsz);
    if (!save_backend_variables(param, client_sock, startup_data, startup_data_len)) {
        pfree(param);
        return -1;  // Failed to save backend variables
    }

    // Step 2: Create unique temporary file name
    snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
             PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
             MyProcPid, ++tmpBackendFileNum);

    // Step 3: Write parameters to temporary file
    fp = AllocateFile(tmpfilename, PG_BINARY_W);
    if (!fp) {
        // Try creating temp directory and retry once
        MakePGDirectory(PG_TEMP_FILES_DIR);
        fp = AllocateFile(tmpfilename, PG_BINARY_W);
        if (!fp) {
            pfree(param);
            return -1;  // Cannot create temp file
        }
    }

    // Write serialized parameters to file
    if (fwrite(param, paramsz, 1, fp) != 1 || FreeFile(fp)) {
        pfree(param);
        return -1;  // Write or close failed
    }
    pfree(param);

    // Step 4: Prepare command line arguments for child process
    argv[0] = "postgres";
    snprintf(forkav, MAXPGPATH, "--forkchild=%s", child_kind);
    argv[1] = forkav;           // Child type identifier
    argv[2] = tmpfilename;      // Parameter file path
    argv[3] = NULL;

    // Step 5: Fork and exec the child process
    if ((pid = fork_process()) == 0) {
        // In child process: execute new postgres instance
        if (execv(postgres_exec_path, argv) < 0) {
            exit(1);  // execv failed, child exits
        }
    }

    return pid;  // Parent returns child PID or -1 on fork failure
}
```

Key simplifications made:
- Removed detailed error reporting for brevity while preserving error handling logic
- Consolidated file I/O error checks into single conditional
- Added descriptive comments for each major step
- Simplified variable declarations and removed static counter details
- Focused on the main execution flow: serialize → write to file → fork → exec
- Abstracted low-level file operations while maintaining the core algorithm