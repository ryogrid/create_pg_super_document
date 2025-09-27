# PostmasterMain

## Location
[src/backend/postmaster/postmaster.c:489-1388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L489-L1388)

## Overview
PostmasterMain is the main entry point for the PostgreSQL postmaster process, responsible for initializing the database system, parsing command-line options, setting up listening sockets, and starting child processes including startup, checkpointer, and background writer.

## Definition

```c
structions.
	 * The postmaster will do the test once at startup, and then its child
	 * processes will inherit the correct function pointer and not need to
	 * repeat the test.
	 */
	LocalProcessControlFile(false);
```
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
  - [InitProcessGlobals](../I/InitProcessGlobals.md): Initialize process-global variables
  - [getInstallationPaths](../g/getInstallationPaths.md): Determine PostgreSQL installation directory paths
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md): Initialize Grand Unified Configuration system
  - [SelectConfigFiles](../S/SelectConfigFiles.md): Locate and read postgresql.conf configuration
  - [checkDataDir](../c/checkDataDir.md): Validate data directory accessibility and permissions
  - [checkControlFile](../c/checkControlFile.md): Verify pg_control file exists and is valid
  - [CreateDataDirLockFile](../C/CreateDataDirLockFile.md): Create postmaster.pid lock file
  - [process_shared_preload_libraries](../p/process_shared_preload_libraries.md): Load shared_preload_libraries modules
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md): Initialize System V IPC resources
  - [CloseServerPorts](../C/CloseServerPorts.md): Socket cleanup function registered with on_proc_exit
  - [ServerLoop](../S/ServerLoop.md): Main postmaster event loop
  - [StartChildProcess](../S/StartChildProcess.md): Launch background processes (startup, checkpointer, bgwriter)
- Called from (representative examples):
  - [main](../m/main.md) (in src/backend/main/main.c:199): Primary entry point from main()

## Notes and Other Information
- This function never returns under normal operation - it either calls ExitPostmaster() for controlled shutdown or abort() for unexpected termination
- Contains extensive platform-specific code for Windows vs. Unix systems, particularly for signal handling and process management
- The function handles both -C (configuration variable output) mode for administrative tools and normal server startup
- Socket creation follows a specific order (TCP before Unix sockets) for reliability of the data directory interlock
- All child processes inherit signal handler configuration, requiring careful coordination with postmaster/bgwriter.c, postmaster/checkpointer.c, etc.
- Supports Bonjour service registration on platforms with DNS-SD support
- The postmaster.pid lock file prevents multiple postmaster instances from running in the same data directory

## Simplified Source

```c
// Simplified version of PostmasterMain
void PostmasterMain(int argc, char *argv[]) {
    // Core initialization step 1: Setup process environment
    InitProcessGlobals();
    PostmasterPid = MyProcPid;
    IsPostmasterEnvironment = true;

    // Core initialization step 2: Setup memory context
    PostmasterContext = AllocSetContextCreate(TopMemoryContext, "Postmaster", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(PostmasterContext);

    // Core initialization step 3: Setup signal handlers
    pqinitmask();
    sigprocmask(SIG_SETMASK, &BlockSig, NULL);
    pqsignal(SIGHUP, handle_pm_reload_request_signal);
    pqsignal(SIGINT, handle_pm_shutdown_request_signal);
    pqsignal(SIGQUIT, handle_pm_shutdown_request_signal);
    pqsignal(SIGTERM, handle_pm_shutdown_request_signal);
    pqsignal(SIGUSR1, handle_pm_pmsignal_signal);
    pqsignal(SIGCHLD, handle_pm_child_exit_signal);
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Core initialization step 4: Initialize configuration system
    getInstallationPaths(argv[0]);
    InitializeGUCOptions();

    // Core logic step 5: Parse command line options
    int opt;
    while ((opt = getopt(argc, argv, "B:bC:c:D:d:EeFf:h:ijk:lN:OPp:r:S:sTt:W:-:")) != -1) {
        switch (opt) {
            case 'B': SetConfigOption("shared_buffers", optarg, PGC_POSTMASTER, PGC_S_ARGV); break;
            case 'D': userDoption = strdup(optarg); break;
            case 'p': SetConfigOption("port", optarg, PGC_POSTMASTER, PGC_S_ARGV); break;
            case 'h': SetConfigOption("listen_addresses", optarg, PGC_POSTMASTER, PGC_S_ARGV); break;
            // ... other options handled similarly
        }
    }

    // Core logic step 6: Load and validate configuration
    if (!SelectConfigFiles(userDoption, progname))
        ExitPostmaster(2);

    checkDataDir();
    checkControlFile();
    ChangeToDataDir();

    // Core logic step 7: Validate configuration settings
    if (SuperuserReservedConnections + ReservedConnections >= MaxConnections)
        ExitPostmaster(1);
    if (XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level == WAL_LEVEL_MINIMAL)
        ereport(ERROR, (errmsg("WAL archival requires wal_level > minimal")));

    // Core logic step 8: Create data directory lock file
    CreateDataDirLockFile(true);

    // Core logic step 9: Initialize shared resources
    LocalProcessControlFile(false);
    ApplyLauncherRegister();
    process_shared_preload_libraries();
    InitializeMaxBackends();
    process_shmem_requests();
    CreateSharedMemoryAndSemaphores();

    // Core logic step 10: Setup network sockets
    ListenSockets = palloc(MAXLISTEN * sizeof(pgsocket));
    on_proc_exit(CloseServerPorts, 0);

    // Setup TCP/IP sockets
    if (ListenAddresses) {
        List *elemlist;
        SplitGUCList(pstrdup(ListenAddresses), ',', &elemlist);
        foreach(l, elemlist) {
            char *curhost = (char *) lfirst(l);
            ListenServerPort(AF_UNSPEC, curhost, PostPortNumber, NULL,
                           ListenSockets, &NumListenSockets, MAXLISTEN);
        }
    }

    // Setup Unix domain sockets
    if (Unix_socket_directories) {
        List *elemlist;
        SplitDirectoriesString(pstrdup(Unix_socket_directories), ',', &elemlist);
        foreach(l, elemlist) {
            char *socketdir = (char *) lfirst(l);
            ListenServerPort(AF_UNIX, NULL, PostPortNumber, socketdir,
                           ListenSockets, &NumListenSockets, MAXLISTEN);
        }
    }

    // Core logic step 11: Final preparations
    RemovePgTempFiles();
    autovac_init();
    load_hba();
    load_ident();

    // Core logic step 12: Start essential background processes
    PgStartTime = GetCurrentTimestamp();
    AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING);

    if (CheckpointerPID == 0)
        CheckpointerPID = StartChildProcess(B_CHECKPOINTER);
    if (BgWriterPID == 0)
        BgWriterPID = StartChildProcess(B_BG_WRITER);

    // Core logic step 13: Start startup process and enter main loop
    StartupPID = StartChildProcess(B_STARTUP);
    StartupStatus = STARTUP_RUNNING;
    pmState = PM_STARTUP;

    maybe_start_bgworkers();

    // Enter main server loop (never returns)
    status = ServerLoop();
    ExitPostmaster(status != STATUS_OK);
}
```

Key simplifications made:
- Removed detailed error handling and platform-specific code (WIN32, Unix differences)
- Consolidated command-line option parsing to show main pattern
- Abstracted complex socket setup logic while preserving the essential flow
- Removed debugging code, environment dumping, and verbose logging
- Simplified configuration validation to key checks only
- Focused on the main execution path without edge cases
- Removed Bonjour registration and other optional features
- Maintained the essential startup sequence and background process creation