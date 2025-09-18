# UserOpts

## Location
[src/bin/pg_upgrade/pg_upgrade.h:330-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.h#L330-L335)

## Overview
UserOpts is a structure that stores user-configurable options for the pg_upgrade utility, controlling various aspects of the PostgreSQL cluster upgrade process.

## Definition
```c
typedef struct
{
    bool        check;          /* check clusters only, don't change any data */
    bool        do_sync;        /* flush changes to disk */
    transferMode transfer_mode; /* copy files or link them? */
    int         jobs;           /* number of processes/threads to use */
    char       *socketdir;      /* directory to use for Unix sockets */
    char       *sync_method;
} UserOpts;
```

## Detailed Description
The UserOpts structure encapsulates all user-configurable options that control the behavior of the pg_upgrade utility. This structure is used throughout the pg_upgrade process to determine how the upgrade should be performed, whether it should be a dry run, how files should be transferred, and various performance and synchronization settings.

## Parameters / Member Variables
- `check`: Boolean flag indicating whether to perform only compatibility checks without making actual changes to data
- `do_sync`: Boolean flag controlling whether to flush changes to disk for data durability
- `transfer_mode`: Enumeration specifying how files should be transferred (copy or hard link)
- `jobs`: Integer specifying the number of parallel processes or threads to use during upgrade
- `socketdir`: String pointer to directory path for Unix domain sockets
- `sync_method`: String pointer specifying the synchronization method to use

## Dependencies
- Functions called/Symbols referenced:
  - transferMode (enum type)
- Called from (representative examples):
  - FIX_DEFAULT_READ_ONLY (in option.c)
  - [OSInfo](../O/OSInfo.md) structure (as part of global context)

## Notes and Other Information
- This structure is typically populated from command-line arguments parsed by the pg_upgrade utility
- The structure is part of the global state used throughout the pg_upgrade process
- The `check` flag is particularly important as it allows users to validate upgrade compatibility without performing actual data migration
- The `jobs` parameter enables parallel processing to improve upgrade performance on multi-core systems