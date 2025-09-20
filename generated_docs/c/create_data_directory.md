# create_data_directory

## Location
[src/bin/initdb/initdb.c:2875-2932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2875-L2932)

## Overview
Creates or validates the PostgreSQL data directory (PGDATA) during database cluster initialization, handling various directory states and ensuring proper permissions.

## Definition

```c
void
create_data_directory(void)
```
## Detailed Description
This function is responsible for setting up the PostgreSQL data directory during the initdb process. It performs comprehensive validation and creation logic based on the current state of the target directory:

1. **Non-existent Directory (case 0)**: Creates the directory with appropriate permissions using  and sets the  flag.

2. **Empty Existing Directory (case 1)**: Fixes permissions on the existing empty directory using  and sets the  flag.

3. **Non-empty Directory (cases 2-4)**: Terminates the initialization process with appropriate error messages. For mount point scenarios (cases 2-3), it calls  to provide additional context.

4. **Inaccessible Directory (default)**: Reports a fatal error if the directory cannot be accessed due to permission or other filesystem issues.

The function ensures that the data directory exists, is empty, and has correct permissions before proceeding with database initialization.

## Parameters / Member Variables
This function operates on global variables:
- : Global variable containing the path to the PostgreSQL data directory
- : Global variable specifying the directory creation permissions
- : Global flag indicating if a new directory was created
- : Global flag indicating if an existing directory was used
- : Global variable containing the program name for error messages

## Dependencies
- Functions called/Symbols referenced:
  - : Checks directory status and contents
  - : Creates directory with parent directories
  - : Prints success message
  - : Provides mount point warnings
  - : Logs detailed error hints
  - , : Standard I/O functions
  - : Changes directory permissions
  - : Logs fatal error and exits
- Called from (representative examples):
  - : Called during main initialization sequence

## Notes and Other Information
- This function is critical to the initdb process and must succeed for initialization to continue
- The function provides user-friendly feedback for each operation performed
- Different return codes from  indicate specific directory conditions that require different handling
- Mount point detection helps prevent accidental initialization on filesystem boundaries
- The function sets global flags that influence subsequent initialization steps
- Proper error handling ensures that users receive clear guidance on resolving directory-related issues
- Directory permissions are set to , which is typically restrictive for security