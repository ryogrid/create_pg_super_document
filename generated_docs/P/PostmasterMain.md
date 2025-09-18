# PostmasterMain

## Location
src/backend/postmaster/postmaster.c: 489 - 1388

## Overview
PostmasterMain is the main entry point for the PostgreSQL postmaster process, responsible for initializing the database system, parsing command-line options, setting up listening sockets, and starting child processes including startup, checkpointer, and background writer.

## Definition


## Detailed Description
PostmasterMain orchestrates the complete initialization and startup sequence of a PostgreSQL database server. It performs critical system-level initialization including:

1. **Process and Signal Setup**: Initializes process globals, sets up signal handlers for proper process management (SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGCHLD), and configures platform-specific signal handling
2. **Memory Context Management**: Creates and switches to the PostmasterContext for memory allocation that can be recycled by backend processes
3. **Command-Line Processing**: Parses extensive command-line options for database configuration, including shared buffers, port numbers, listen addresses, SSL settings, and debug options
4. **Configuration and Validation**: Loads configuration files, validates data directory, checks control file, and performs sanity checks on GUC settings
5. **Network Setup**: Establishes TCP/IP and Unix domain sockets for client connections, handles Bonjour registration on supported platforms
6. **Child Process Management**: Starts essential background processes including startup process, checkpointer, and background writer
7. **Resource Initialization**: Sets up shared memory, semaphores, file descriptors, and other system resources

The function concludes by entering ServerLoop() to handle ongoing postmaster operations and never returns under normal circumstances.

## Parameters / Member Variables
- : Number of command-line arguments passed to the postmaster
- : Array of command-line argument strings, where argv[0] is used to determine installation paths

## Dependencies
- Functions called/Symbols referenced:
  - InitProcessGlobals: Initialize process-global variables
  - getInstallationPaths: Determine PostgreSQL installation directory paths
  - InitializeGUCOptions: Initialize Grand Unified Configuration system
  - SelectConfigFiles: Locate and read postgresql.conf configuration
  - checkDataDir: Validate data directory accessibility and permissions
  - checkControlFile: Verify pg_control file exists and is valid
  - CreateDataDirLockFile: Create postmaster.pid lock file
  - process_shared_preload_libraries: Load shared_preload_libraries modules
  - CreateSharedMemoryAndSemaphores: Initialize System V IPC resources
  - CloseServerPorts: Socket cleanup function registered with on_proc_exit
  - ServerLoop: Main postmaster event loop
  - StartChildProcess: Launch background processes (startup, checkpointer, bgwriter)
- Called from (representative examples):
  - main (in src/backend/main/main.c:199): Primary entry point from main()

## Notes and Other Information
- This function never returns under normal operation - it either calls ExitPostmaster() for controlled shutdown or abort() for unexpected termination
- Contains extensive platform-specific code for Windows vs. Unix systems, particularly for signal handling and process management
- The function handles both -C (configuration variable output) mode for administrative tools and normal server startup
- Socket creation follows a specific order (TCP before Unix sockets) for reliability of the data directory interlock
- All child processes inherit signal handler configuration, requiring careful coordination with postmaster/bgwriter.c, postmaster/checkpointer.c, etc.
- Supports Bonjour service registration on platforms with DNS-SD support
- The postmaster.pid lock file prevents multiple postmaster instances from running in the same data directory