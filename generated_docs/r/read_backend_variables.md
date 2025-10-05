# read_backend_variables

## Location
[src/backend/postmaster/launch_backend.c:883-976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L883-L976)

## Overview
Reads backend parameters and startup data from either a file (Unix/Linux) or shared memory mapping (Windows) that was previously saved by the postmaster process.

## Definition
```c
static void read_backend_variables(char *id, char **startup_data, size_t *startup_data_len)
```

## Detailed Description
This function implements platform-specific mechanisms to retrieve backend parameters and startup data that were previously stored by save_backend_variables(). On Unix/Linux systems, it reads from a temporary file and then deletes it. On Windows, it uses memory-mapped files for inter-process communication. The function reads the BackendParameters structure first, then reads the variable-length startup data if present. After successfully reading the data, it calls restore_backend_variables() to apply the parameters to the current process.

## Parameters / Member Variables
- `id`: Platform-specific identifier - filename on Unix/Linux, handle value as string on Windows
- `startup_data`: Pointer to a char pointer that will be set to point to the allocated startup data
- `startup_data_len`: Pointer to size_t that will be set to the length of the startup data

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md) (Unix/Linux only)
  - [FreeFile](../F/FreeFile.md) (Unix/Linux only) 
  - [write_stderr](../w/write_stderr.md)
  - exit
  - [palloc](../p/palloc.md)
  - unlink (Unix/Linux only)
  - MapViewOfFile (Windows only)
  - UnmapViewOfFile (Windows only)
  - CloseHandle (Windows only)
  - GetLastError (Windows only)
  - memcpy (Windows only)
  - atol/_atoi64 (Windows only)
  - [restore_backend_variables](restore_backend_variables.md)
  - [BackendParameters](../B/BackendParameters.md) (structure type)
  - PG_BINARY_R (constant)
- Called from (representative examples):
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - SizeOfBackendParameters

## Notes and Other Information
- Uses conditional compilation for platform-specific implementations (#ifndef WIN32)
- Unix/Linux version uses temporary files that are deleted after reading
- Windows version uses memory-mapped files identified by handle values
- On Windows, uses _atoi64 for 64-bit builds and atol for 32-bit builds to convert handle strings
- All file operations and memory operations are critical - failures result in process exit with code 1
- Automatically allocates memory for startup data using palloc() when startup_data_len > 0
- Sets startup_data to NULL when no startup data is present
- Always calls restore_backend_variables() at the end to apply the loaded parameters
- Part of the backend launch mechanism that enables parameter passing from postmaster to backend processes

## Simplified Source

```c
static void read_backend_variables(char *id, char **startup_data, size_t *startup_data_len) {
    BackendParameters param;

#ifndef WIN32
    // Unix/Linux: Read from temporary file
    FILE *fp = AllocateFile(id, PG_BINARY_R);
    if (!fp) {
        write_stderr("could not open backend variables file \"%s\": %m\n", id);
        exit(1);
    }

    // Read backend parameters structure
    if (fread(&param, sizeof(param), 1, fp) != 1) {
        write_stderr("could not read from backend variables file \"%s\": %m\n", id);
        exit(1);
    }

    // Read startup data if present
    *startup_data_len = param.startup_data_len;
    if (param.startup_data_len > 0) {
        *startup_data = palloc(*startup_data_len);
        if (fread(*startup_data, *startup_data_len, 1, fp) != 1) {
            write_stderr("could not read startup data from file \"%s\": %m\n", id);
            exit(1);
        }
    } else {
        *startup_data = NULL;
    }

    // Cleanup: close file and delete it
    FreeFile(fp);
    unlink(id);

#else
    // Windows: Use memory-mapped file
    HANDLE paramHandle = (HANDLE) _atoi64(id);  // or atol() for 32-bit
    BackendParameters *paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
    if (!paramp) {
        write_stderr("could not map view of backend variables\n");
        exit(1);
    }

    // Copy parameters and startup data
    memcpy(&param, paramp, sizeof(BackendParameters));
    *startup_data_len = param.startup_data_len;
    if (param.startup_data_len > 0) {
        *startup_data = palloc(param.startup_data_len);
        memcpy(*startup_data, paramp->startup_data, param.startup_data_len);
    } else {
        *startup_data = NULL;
    }

    // Cleanup: unmap and close handle
    UnmapViewOfFile(paramp);
    CloseHandle(paramHandle);
#endif

    // Apply the loaded parameters to current process
    restore_backend_variables(&param);
}
```