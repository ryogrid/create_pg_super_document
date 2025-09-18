# _SubscriptionInfo

## Location
[src/bin/pg_dump/pg_dump.h:671-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L671-L688)

## Overview
The `_SubscriptionInfo` struct represents a logical replication subscription, used by pg_dump to store comprehensive configuration and metadata about subscriptions.

## Definition
```c
typedef struct _SubscriptionInfo
{
    DumpableObject dobj;
    const char *rolname;
    char       *subenabled;
    char       *subbinary;
    char       *substream;
    char       *subtwophasestate;
    char       *subdisableonerr;
    char       *subpasswordrequired;
    char       *subrunasowner;
    char       *subconninfo;
    char       *subslotname;
    char       *subsynccommit;
    char       *subpublications;
    char       *suborigin;
    char       *suboriginremotelsn;
    char       *subfailover;
} SubscriptionInfo;
```

## Detailed Description
This structure is part of PostgreSQL's pg_dump utility and contains all the configuration parameters and metadata for a logical replication subscription. It stores information about how the subscription connects to the publisher, what publications it subscribes to, and various behavioral settings like binary transfer mode, streaming options, and error handling. This comprehensive information is essential for accurately recreating subscription configurations during database dumps and restores.

## Parameters / Member Variables
- `dobj`: Base DumpableObject structure containing common metadata for dump objects
- `rolname`: Name of the role that owns the subscription
- `subenabled`: String indicating whether the subscription is enabled ('t'/'f')
- `subbinary`: String indicating whether binary transfer mode is enabled ('t'/'f')
- `substream`: String indicating streaming mode setting for large transactions
- `subtwophasestate`: String indicating two-phase commit state ('d'/'p'/'e' for disabled/pending/enabled)
- `subdisableonerr`: String indicating whether to disable subscription on error ('t'/'f')
- `subpasswordrequired`: String indicating whether password is required for connection ('t'/'f')
- `subrunasowner`: String indicating whether to run as subscription owner ('t'/'f')
- `subconninfo`: Connection string for connecting to the publisher database
- `subslotname`: Name of the replication slot on the publisher
- `subsynccommit`: Synchronous commit setting for the subscription
- `subpublications`: Comma-separated list of publication names this subscription subscribes to
- `suborigin`: Origin name for the subscription (used for conflict detection)
- `suboriginremotelsn`: Remote LSN associated with the origin
- `subfailover`: String indicating whether failover is enabled ('t'/'f')

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_dump.h at lines 671-688
- It's used specifically by the pg_dump utility for logical replication subscription handling
- Many fields are stored as strings rather than booleans to maintain compatibility with the text-based nature of SQL dumps
- The structure reflects the comprehensive nature of PostgreSQL subscription configuration with many advanced features
- This is one of the more complex dump structures due to the rich feature set of logical replication subscriptions