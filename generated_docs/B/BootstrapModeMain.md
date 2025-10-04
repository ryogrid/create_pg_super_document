# BootstrapModeMain

## Location
[src/backend/bootstrap/bootstrap.c:199-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L199-L380)

## Overview
BootstrapModeMain is the main entry point for running PostgreSQL in bootstrap mode, responsible for initializing the template database and processing bootstrap commands in a special bootstrap language rather than SQL.

## Definition
```c
void BootstrapModeMain(int argc, char *argv[], bool check_only)
```

## Detailed Description
BootstrapModeMain serves as the core function for PostgreSQL's bootstrap mode, which is used to initialize the template database during PostgreSQL installation. Unlike normal PostgreSQL operation that processes SQL commands, bootstrap mode uses a special bootstrap language for database initialization.

The function handles command-line argument parsing, configuration setup, shared memory initialization, and the complete bootstrap process. When check_only is true, it performs minimal startup operations just to verify configuration validity, particularly shared memory settings, without proceeding to full database initialization.

Key operations include:
- Command-line argument processing with various bootstrap-specific flags
- GUC (Grand Unified Configuration) option initialization and validation
- Data directory validation and lock file creation
- Shared memory and semaphore creation
- Process initialization and signal handling setup
- Bootstrap file parsing and processing
- Transaction management during bootstrap operations
- Cleanup and proper process termination

## Parameters / Member Variables
- `argc`: Number of command-line arguments
- `argv`: Array of command-line argument strings
- `check_only`: Boolean flag indicating whether to perform only configuration validation (true) or full bootstrap initialization (false)

## Dependencies
- Functions called/Symbols referenced:
  - [InitStandaloneProcess](../I/InitStandaloneProcess.md)
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - [SelectConfigFiles](../S/SelectConfigFiles.md)
  - [checkDataDir](../c/checkDataDir.md)
  - [ChangeToDataDir](../C/ChangeToDataDir.md)
  - [CreateDataDirLockFile](../C/CreateDataDirLockFile.md)
  - SetProcessingMode
  - [InitializeMaxBackends](../I/InitializeMaxBackends.md)
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)
  - [CheckerModeMain](../C/CheckerModeMain.md) (when check_only is true)
  - [InitProcess](../I/InitProcess.md)
  - [BaseInit](BaseInit.md)
  - [bootstrap_signals](../b/bootstrap_signals.md)
  - [BootStrapXLOG](BootStrapXLOG.md)
  - [InitPostgres](../I/InitPostgres.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - boot_yyparse
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [RelationMapFinishBootstrap](../R/RelationMapFinishBootstrap.md)
  - [cleanup](../c/cleanup.md)
  - [proc_exit](../p/proc_exit.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/backend/main/main.c)

## Notes and Other Information
- This function is only called when PostgreSQL is not running under the postmaster (Assert(!IsUnderPostmaster))
- Supports various command-line options including -B (shared_buffers), -c (config options), -D (data directory), -d (debug), -F (disable fsync), -k (enable checksums), -r (output file), and -X (WAL segment size)
- In check_only mode, the function calls CheckerModeMain() and then aborts, used for configuration validation
- Sets IgnoreSystemIndexes to true during bootstrap processing
- Initializes attribute arrays (attrtypes, Nulls) for bootstrap file processing
- Located in src/backend/bootstrap/bootstrap.c:199-380
- The bootstrap language processing is handled by boot_yyparse(), which is generated from a yacc grammar

## Simplified Source
```c
void BootstrapModeMain(int argc, char *argv[], bool check_only) {
    char *progname = argv[0];
    char *userDoption = NULL;

    Assert(!IsUnderPostmaster);
    InitStandaloneProcess(argv[0]);
    InitializeGUCOptions();

    // Parse command line arguments
    while ((flag = getopt(argc, argv, "B:c:d:D:Fkr:X:-:")) != -1) {
        switch (flag) {
            case 'B': SetConfigOption("shared_buffers", optarg, PGC_POSTMASTER, PGC_S_ARGV); break;
            case 'c': /* Parse and set config option */ break;
            case 'D': userDoption = pstrdup(optarg); break;
            case 'd': /* Set debug logging */ break;
            case 'F': SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV); break;
            case 'k': bootstrap_data_checksum_version = PG_DATA_CHECKSUM_VERSION; break;
            case 'r': strlcpy(OutputFileName, optarg, MAXPGPATH); break;
            case 'X': SetConfigOption("wal_segment_size", optarg, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT); break;
            default: write_stderr("Try \"%s --help\" for more information.\n", progname); proc_exit(1);
        }
    }

    // Configuration and directory setup
    if (!SelectConfigFiles(userDoption, progname)) proc_exit(1);
    checkDataDir();
    ChangeToDataDir();
    CreateDataDirLockFile(false);

    // Initialize bootstrap processing
    SetProcessingMode(BootstrapProcessing);
    IgnoreSystemIndexes = true;
    InitializeMaxBackends();
    CreateSharedMemoryAndSemaphores();

    // Check-only mode: validate configuration and exit
    if (check_only) {
        SetProcessingMode(NormalProcessing);
        CheckerModeMain();
        abort();
    }

    // Full bootstrap initialization
    InitProcess();
    BaseInit();
    bootstrap_signals();
    BootStrapXLOG();
    InitPostgres(NULL, InvalidOid, NULL, InvalidOid, 0, NULL);

    // Initialize attribute processing arrays
    for (int i = 0; i < MAXATTR; i++) {
        attrtypes[i] = NULL;
        Nulls[i] = false;
    }

    // Process bootstrap input file
    StartTransactionCommand();
    boot_yyparse();
    CommitTransactionCommand();

    // Finalize and cleanup
    RelationMapFinishBootstrap();
    cleanup();
    proc_exit(0);
}
```