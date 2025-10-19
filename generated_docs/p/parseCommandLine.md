# parseCommandLine

## Location
[src/bin/pg_upgrade/option.c:39-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/option.c#L39-L281)

## Overview
Parses command line arguments for the pg_upgrade utility and populates configuration structures with user-specified options.

## Definition
```c
void parseCommandLine(int argc, char *argv[])
```

## Detailed Description
This function is the core command-line argument parser for pg_upgrade, handling all configuration options that control the upgrade process. It uses getopt_long() to parse both short and long options, validating input and setting up global structures for old and new cluster information, transfer modes, and various operational flags. The function also performs environment variable processing, privilege checks, and essential validation to ensure the upgrade can proceed safely.

## Parameters / Member Variables
- `argc`: Number of command line arguments
- `argv[]`: Array of command line argument strings

## Dependencies
- Functions called/Symbols referenced:
  - [get_progname](../g/get_progname.md)
  - [get_user_info](../g/get_user_info.md)
  - [usage](../u/usage.md)
  - [getopt_long](../g/getopt_long.md)
  - [pg_strdup](pg_strdup.md)
  - [pg_free](pg_free.md)
  - [pg_log](pg_log.md)
  - [check_required_directory](../c/check_required_directory.md)
  - [parse_sync_method](parse_sync_method.md)
  - setenv
  - [canonicalize_path](../c/canonicalize_path.md) (Windows)
  - [path_is_prefix_of_path](path_is_prefix_of_path.md) (Windows)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:103)

## Notes and Other Information
- Supports comprehensive set of options including data directories (-d/-D), binary directories (-b/-B), ports (-p/-P), transfer modes (--link, --clone, --copy), and operational flags
- Performs security check to prevent running as root user
- Handles environment variables (PGPORTOLD, PGPORTNEW, PGUSER, PGOPTIONS) with proper defaults
- On Windows, includes special validation to prevent running from inside the new cluster directory
- Sets up PGOPTIONS environment variable with FIX_DEFAULT_READ_ONLY to handle read-only mode
- Uses check_required_directory to validate and set directory paths from command line or environment variables
- Transfer modes include COPY (default), LINK, CLONE, and COPY_FILE_RANGE options

## Simplified Source

```c
void
parseCommandLine(int argc, char *argv[])
{
    static struct option long_options[] = {
        {"old-datadir", required_argument, NULL, 'd'},
        {"new-datadir", required_argument, NULL, 'D'},
        {"old-bindir", required_argument, NULL, 'b'},
        {"new-bindir", required_argument, NULL, 'B'},
        {"check", no_argument, NULL, 'c'},
        {"link", no_argument, NULL, 'k'},
        {"jobs", required_argument, NULL, 'j'},
        {"verbose", no_argument, NULL, 'v'},
        // ... other options ...
        {NULL, 0, NULL, 0}
    };

    int option;
    int optindex = 0;

    // Set defaults
    user_opts.do_sync = true;
    user_opts.transfer_mode = TRANSFER_MODE_COPY;
    os_info.progname = get_progname(argv[0]);

    // Process environment variables for ports and user
    old_cluster.port = getenv("PGPORTOLD") ? atoi(getenv("PGPORTOLD")) : DEF_PGUPORT;
    new_cluster.port = getenv("PGPORTNEW") ? atoi(getenv("PGPORTNEW")) : DEF_PGUPORT;

    // Get user info and check PGUSER override
    get_user_info(&os_info.user);
    if (getenv("PGUSER")) {
        pg_free(os_info.user);
        os_info.user = pg_strdup(getenv("PGUSER"));
    }

    // Handle help and version requests
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("pg_upgrade (PostgreSQL) " PG_VERSION);
            exit(0);
        }
    }

    // Security check - cannot run as root
    if (get_user_effective_id() == 0)
        pg_fatal("%s: cannot be run as root", os_info.progname);

    // Parse command line options
    while ((option = getopt_long(argc, argv, "b:B:cd:D:j:kNo:O:p:P:rs:U:v",
                                 long_options, &optindex)) != -1) {
        switch (option) {
            case 'b': old_cluster.bindir = pg_strdup(optarg); break;
            case 'B': new_cluster.bindir = pg_strdup(optarg); break;
            case 'c': user_opts.check = true; break;
            case 'd': old_cluster.pgdata = pg_strdup(optarg); break;
            case 'D': new_cluster.pgdata = pg_strdup(optarg); break;
            case 'j': user_opts.jobs = atoi(optarg); break;
            case 'k': user_opts.transfer_mode = TRANSFER_MODE_LINK; break;
            case 'p': old_cluster.port = atoi(optarg); break;
            case 'P': new_cluster.port = atoi(optarg); break;
            case 'v': log_opts.verbose = true; break;
            case 'U':
                pg_free(os_info.user);
                os_info.user = pg_strdup(optarg);
                break;
            // Handle other options...
            default:
                fprintf(stderr, "Try \"%s --help\" for more information.\n", os_info.progname);
                exit(1);
        }
    }

    // Validate remaining arguments
    if (optind < argc)
        pg_fatal("too many command-line arguments (first is \"%s\")", argv[optind]);

    // Set defaults and environment
    if (!user_opts.sync_method)
        user_opts.sync_method = pg_strdup("fsync");

    // Configure PGOPTIONS environment for read-only handling
    if (getenv("PGOPTIONS")) {
        char *pgoptions = psprintf("%s %s", FIX_DEFAULT_READ_ONLY, getenv("PGOPTIONS"));
        setenv("PGOPTIONS", pgoptions, 1);
        pfree(pgoptions);
    } else {
        setenv("PGOPTIONS", FIX_DEFAULT_READ_ONLY, 1);
    }

    // Validate required directories
    check_required_directory(&old_cluster.bindir, "PGBINOLD", false, "-b", "old cluster binaries reside", false);
    check_required_directory(&new_cluster.bindir, "PGBINNEW", false, "-B", "new cluster binaries reside", true);
    check_required_directory(&old_cluster.pgdata, "PGDATAOLD", false, "-d", "old cluster data resides", false);
    check_required_directory(&new_cluster.pgdata, "PGDATANEW", false, "-D", "new cluster data resides", false);
}
```