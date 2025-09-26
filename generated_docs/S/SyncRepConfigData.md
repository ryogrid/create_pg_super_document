# SyncRepConfigData

## Location
[src/include/replication/syncrep.h:63-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/syncrep.h#L63-L72)

## Overview
SyncRepConfigData is a struct that holds the configuration data for PostgreSQL synchronous replication, storing the parsed representation of the synchronous_standby_names GUC parameter.

## Definition
```c
typedef struct SyncRepConfigData
{
    int     config_size;    /* total size of this struct, in bytes */
    int     num_sync;       /* number of sync standbys that we need to
                             * wait for */
    uint8   syncrep_method; /* method to choose sync standbys */
    int     nmembers;       /* number of members in the following list */
    /* member_names contains nmembers consecutive nul-terminated C strings */
    char    member_names[FLEXIBLE_ARRAY_MEMBER];
} SyncRepConfigData;
```

## Detailed Description
SyncRepConfigData represents the parsed and validated configuration for synchronous replication in PostgreSQL. This struct is designed as a flat representation that can be stored in a single chunk of malloc'd memory, making it suitable for storage as the "extra" data associated with the synchronous_standby_names GUC parameter. The struct contains both the configuration metadata (such as the number of required sync standbys and the method for choosing them) and the actual standby names in a flexible array member. This design allows the configuration to be efficiently stored, retrieved, and passed around within the synchronous replication subsystem.

## Parameters / Member Variables
- `config_size`: Total size of this struct in bytes, including the variable-length member_names array
- `num_sync`: Number of synchronous standbys that the primary server needs to wait for before considering a transaction committed
- `syncrep_method`: Method used to choose synchronous standbys (e.g., FIRST or ANY priority method)
- `nmembers`: Number of standby server names stored in the member_names array
- `member_names`: Flexible array member containing nmembers consecutive null-terminated C strings representing the names of potential synchronous standbys

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - SyncStandbysDefined
  - [check_synchronous_standby_names](../c/check_synchronous_standby_names.md)
  - [assign_synchronous_standby_names](../a/assign_synchronous_standby_names.md)

## Notes and Other Information
- This struct must maintain a flat memory layout to be compatible with PostgreSQL's GUC (Grand Unified Configuration) system
- The flexible array member allows for variable numbers of standby names without requiring separate memory allocations
- The struct is used internally to represent the parsed form of the synchronous_standby_names configuration parameter
- The syncrep_method field determines whether the system uses FIRST (priority-based) or ANY (quorum-based) synchronous replication
- Memory layout is critical since this struct is stored as GUC extra data and must be copyable as a single memory block