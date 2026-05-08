# Appendix — Key Data Structures

[Up: index.md](index.md)  |  [Prev: appendix_glossary.md](appendix_glossary.md)  |  [Next: appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md)

This appendix collects the C struct definitions referenced throughout
the document. Field comments are abridged; consult the source for
absolute authority.

## Cluster anchor: `ControlFileData` and `CheckPoint`

```c
/* src/include/catalog/pg_control.h:35 */
typedef struct CheckPoint
{
    XLogRecPtr        redo;            /* REDO start point */
    TimeLineID        ThisTimeLineID;
    TimeLineID        PrevTimeLineID;
    bool              fullPageWrites;
    int               wal_level;
    FullTransactionId nextXid;
    Oid               nextOid;
    MultiXactId       nextMulti;
    MultiXactOffset   nextMultiOffset;
    TransactionId     oldestXid;
    Oid               oldestXidDB;
    MultiXactId       oldestMulti;
    Oid               oldestMultiDB;
    pg_time_t         time;
    TransactionId     oldestCommitTsXid;
    TransactionId     newestCommitTsXid;
    TransactionId     oldestActiveXid;
} CheckPoint;
```

```c
/* src/include/catalog/pg_control.h:104 */
typedef struct ControlFileData
{
    uint64       system_identifier;
    uint32       pg_control_version;     /* PG_CONTROL_VERSION = 1700 */
    uint32       catalog_version_no;     /* CATALOG_VERSION_NO from catversion.h */
    DBState      state;                  /* DB_STARTUP, DB_IN_PRODUCTION, ... */
    pg_time_t    time;                   /* time of last update */
    XLogRecPtr   checkPoint;             /* LSN of last checkpoint record */
    CheckPoint   checkPointCopy;         /* copy of last checkpoint */

    XLogRecPtr   unloggedLSN;            /* counter for unlogged-rel init forks */

    /* These two fields are only valid during recovery; meaningless else */
    XLogRecPtr   minRecoveryPoint;
    TimeLineID   minRecoveryPointTLI;

    XLogRecPtr   backupStartPoint;
    XLogRecPtr   backupEndPoint;
    bool         backupEndRequired;

    /* WAL parameters captured for standby compatibility */
    int          wal_level;
    bool         wal_log_hints;
    int          MaxConnections;
    int          max_worker_processes;
    int          max_wal_senders;
    int          max_prepared_xacts;
    int          max_locks_per_xact;
    bool         track_commit_timestamp;

    /* Architecture compatibility */
    uint32       maxAlign;
    double       floatFormat;
    uint32       blcksz;
    uint32       relseg_size;
    uint32       xlog_blcksz;
    uint32       xlog_seg_size;
    uint32       nameDataLen;
    uint32       indexMaxKeys;
    uint32       toast_max_chunk_size;
    uint32       loblksize;
    bool         float8ByVal;

    uint32       data_checksum_version;

    char         mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];

    pg_crc32c    crc;
} ControlFileData;

/* src/include/catalog/pg_control.h:241 */
#define PG_CONTROL_MAX_SAFE_SIZE   512
/* src/include/catalog/pg_control.h:250 */
#define PG_CONTROL_FILE_SIZE       8192
```

## Relcache: `RelationData`

```c
/* src/include/utils/rel.h (abridged) */
typedef struct RelationData
{
    RelFileLocator  rd_locator;          /* (spcOid, dbOid, relNumber) */
    SMgrRelation    rd_smgr;
    int             rd_refcnt;
    ProcNumber      rd_backend;          /* for temp rels */
    bool            rd_islocaltemp;
    bool            rd_isnailed;
    bool            rd_isvalid;
    bool            rd_indexvalid;
    bool            rd_statvalid;
    SubTransactionId rd_createSubid;
    SubTransactionId rd_newRelfilelocatorSubid;
    SubTransactionId rd_firstRelfilelocatorSubid;
    SubTransactionId rd_droppedSubid;

    Form_pg_class   rd_rel;              /* pg_class row, palloc'd */
    TupleDesc       rd_att;
    Oid             rd_id;
    LockInfoData    rd_lockInfo;
    RuleLock       *rd_rules;
    MemoryContext   rd_rulescxt;
    TriggerDesc    *trigdesc;
    /* row security policies */
    RowSecurityDesc *rd_rsdesc;
    List           *rd_fkeylist;
    bool            rd_fkeyvalid;

    MemoryContext   rd_partkeycxt;
    PartitionKey    rd_partkey;
    MemoryContext   rd_pdcxt;
    PartitionDesc   rd_partdesc;
    PartitionDesc   rd_partdesc_nodetached;
    PartitionDirectory rd_partdir;

    List           *rd_indexlist;
    Bitmapset      *rd_indexattr;
    Bitmapset      *rd_keyattr;
    Bitmapset      *rd_pkattr;
    Bitmapset      *rd_idattr;
    Bitmapset      *rd_hotblockingattr;
    Bitmapset      *rd_summarizedattr;

    Oid             rd_oidindex;          /* OID-keyed index Oid */
    Oid             rd_pkindex;
    bool            rd_ispkdeferrable;
    Oid             rd_replidindex;

    PublicationDesc *rd_pubdesc;

    bytea          *rd_options;            /* parsed reloptions */

    /* Index-only fields */
    Form_pg_index   rd_index;
    struct HeapTupleData *rd_indextuple;
    Form_pg_am      rd_amhandler;
    Oid             rd_indcollation[INDEX_MAX_KEYS];

    MemoryContext   rd_indexcxt;
    RelationAmInfo *rd_amcache;            /* per-AM scratch */
    void           *rd_amroutine;          /* IndexAmRoutine or TableAmRoutine */
    /* ... more fields ... */
} RelationData;

typedef RelationData *Relation;
```

## Catcache: `CatCache`, `CatCTup`, `CatCList`

```c
/* src/include/utils/catcache.h */
typedef struct catcache
{
    int          id;                       /* SysCacheIdentifier */
    int          cc_nbuckets;
    TupleDesc    cc_tupdesc;
    int          cc_reloid;
    int          cc_indexoid;
    int          cc_relisshared;
    bool         cc_relisrelmapped;
    int          cc_ntup;
    int          cc_nlist;
    int          cc_nkeys;
    int16        cc_keyno[CATCACHE_MAXKEYS];
    PGFunction   cc_hashfunc[CATCACHE_MAXKEYS];
    Oid          cc_skey[CATCACHE_MAXKEYS];
    bool         cc_isname[CATCACHE_MAXKEYS];
    dlist_head  *cc_bucket;                /* hash buckets */
    dlist_head   cc_lists;                  /* CatCList list */
    /* statistics */
    int          cc_searches;
    int          cc_hits;
    int          cc_neg_hits;
    int          cc_newloads;
    int          cc_invals;
    /* ... */
} CatCache;

typedef struct catctup
{
    int             ct_magic;
    CatCache       *my_cache;
    dlist_node      cache_elem;             /* link in cc_bucket */
    Datum           keys[CATCACHE_MAXKEYS];
    uint32          hash_value;
    bool            negative;               /* true = "no such row" entry */
    bool            dead;
    int             refcount;
    HeapTupleData   tuple;                  /* the cached row */
} CatCTup;

typedef struct catclist
{
    int             cl_magic;
    CatCache       *my_cache;
    dlist_node      cache_elem;
    int             refcount;
    bool            dead;
    bool            ordered;
    short           nkeys;                  /* # of partial keys */
    Datum           keys[CATCACHE_MAXKEYS]; /* partial key */
    uint32          hash_value;
    int             n_members;
    CatCTup        *members[FLEXIBLE_ARRAY_MEMBER];
} CatCList;
```

## SLRU: `SlruSharedData`, `SlruCtlData`

```c
/* src/include/access/slru.h:47 */
typedef enum
{
    SLRU_PAGE_EMPTY,
    SLRU_PAGE_READ_IN_PROGRESS,
    SLRU_PAGE_VALID,
    SLRU_PAGE_WRITE_IN_PROGRESS,
} SlruPageStatus;

/* src/include/access/slru.h:61 */
typedef struct SlruSharedData
{
    int               num_slots;
    char            **page_buffer;          /* num_slots BLCKSZ buffers */
    SlruPageStatus   *page_status;
    bool             *page_dirty;
    int64            *page_number;
    int              *page_lru_count;

    LWLockPadded     *buffer_locks;         /* per-slot I/O lock */
    LWLockPadded     *bank_locks;           /* per-bank slot-access lock */
    int              *bank_cur_lru_count;

    /* CLOG only */
    XLogRecPtr       *group_lsn;
    int               lsn_groups_per_page;

    pg_atomic_uint64  latest_page_number;
    int               slru_stats_idx;
} SlruSharedData;

typedef SlruSharedData *SlruShared;

/* src/include/access/slru.h:127 */
typedef struct SlruCtlData
{
    SlruShared        shared;
    uint16            nbanks;
    bool              long_segment_names;
    SyncRequestHandler sync_handler;
    bool             (*PagePrecedes)(int64 a, int64 b);
    char              Dir[64];              /* relative to PGDATA */
} SlruCtlData;

typedef SlruCtlData *SlruCtl;
```

## Relmapper: `RelMapping`, `RelMapFile`

```c
/* src/backend/utils/cache/relmapper.c */
typedef struct RelMapping
{
    Oid           mapoid;
    RelFileNumber mapfilenumber;
} RelMapping;

#define MAX_MAPPINGS  64

typedef struct RelMapFile
{
    int32       magic;                       /* RELMAPPER_FILEMAGIC = 0x592717 */
    int32       num_mappings;
    RelMapping  mappings[MAX_MAPPINGS];
    pg_crc32c   crc;
} RelMapFile;
```

## CommitTs: `CommitTimestampEntry`

```c
/* src/backend/access/transam/commit_ts.c */
typedef struct CommitTimestampEntry
{
    TimestampTz time;                        /* 8 bytes */
    RepOriginId nodeid;                      /* 2 bytes */
} CommitTimestampEntry;

#define COMMIT_TS_XACTS_PER_PAGE  (BLCKSZ / SizeOfCommitTimestampEntry)  /* 819 */
```

## MultiXact: `MultiXactStatus`, `MultiXactMember`

```c
/* src/include/access/multixact.h:37 */
typedef enum
{
    MultiXactStatusForKeyShare = 0x00,
    MultiXactStatusForShare,
    MultiXactStatusForNoKeyUpdate,
    MultiXactStatusForUpdate,
    MultiXactStatusNoKeyUpdate = 0x04,
    MultiXactStatusUpdate
} MultiXactStatus;

#define ISUPDATE_from_mxstatus(status)  (((status) & 0x04) != 0)

typedef struct MultiXactMember
{
    TransactionId   xid;
    MultiXactStatus status;
} MultiXactMember;
```

## Cache invalidation: `SharedInvalidationMessage`

```c
/* src/include/storage/sinval.h (one-of-many union form) */
typedef struct
{
    int8         id;                         /* SHAREDINVALCATCACHE_ID etc. */
    int8         cacheId;
    uint32       hashValue;
    Oid          dbId;
} SharedInvalCatcacheMsg;

typedef struct
{
    int8         id;                         /* SHAREDINVALRELCACHE_ID */
    Oid          dbId;
    Oid          relId;
} SharedInvalRelcacheMsg;

typedef struct
{
    int8         id;                         /* SHAREDINVALSMGR_ID */
    int8         backend_hi;
    int16        backend_lo;
    RelFileLocator rlocator;
} SharedInvalSmgrMsg;

typedef struct
{
    int8         id;                         /* SHAREDINVALRELMAP_ID */
    Oid          dbId;
} SharedInvalRelmapMsg;

typedef struct
{
    int8         id;                         /* SHAREDINVALSNAPSHOT_ID */
    int8         cacheId;
    Oid          dbId;
    Oid          relId;
} SharedInvalSnapshotMsg;

typedef union
{
    int8                       id;
    SharedInvalCatcacheMsg     cc;
    SharedInvalCatalogMsg      cat;
    SharedInvalRelcacheMsg     rc;
    SharedInvalSmgrMsg         sm;
    SharedInvalRelmapMsg       rm;
    SharedInvalSnapshotMsg     sn;
} SharedInvalidationMessage;
```

## sinval ring buffer: `SharedInvalidStateData`, `ProcState`

```c
/* src/include/storage/sinvaladt.h */
typedef struct ProcState
{
    int        nextMsgNum;       /* next message I have not read */
    bool       resetState;       /* true: SI_RESET pending */
    bool       signaled;
    bool       hasMessages;
    pid_t      pid;
    int        proc;
    bool       sendOnly;
} ProcState;

typedef struct SharedInvalidStateData
{
    int     minMsgNum;
    int     maxMsgNum;
    int     nextThreshold;
    int     lastBackend;
    int     maxBackends;
    LWLock  SInvalReadLock;
    LWLock  SInvalWriteLock;
    int     numFreeBackends;

    SharedInvalidationMessage  buffer[MAXNUMMESSAGES];
    ProcState                  procState[FLEXIBLE_ARRAY_MEMBER];
} SharedInvalidStateData;
```

## WAL record payloads

### `xl_clog_truncate` (clog.h:32)

```c
typedef struct xl_clog_truncate
{
    int64           pageno;
    TransactionId   oldestXact;
    Oid             oldestXactDb;
} xl_clog_truncate;
```

### `xl_multixact_create` (multixact.h)

```c
typedef struct xl_multixact_create
{
    MultiXactId        mid;
    MultiXactOffset    moff;
    int32              nmembers;
    MultiXactMember    members[FLEXIBLE_ARRAY_MEMBER];
} xl_multixact_create;
```

### `xl_multixact_truncate` (multixact.h)

```c
typedef struct xl_multixact_truncate
{
    Oid              oldestMultiDB;
    MultiXactId      startTruncOff;
    MultiXactId      endTruncOff;
    MultiXactOffset  startTruncMemb;
    MultiXactOffset  endTruncMemb;
} xl_multixact_truncate;
```

### `xl_relmap_update` (relmapper.h:27)

```c
typedef struct xl_relmap_update
{
    Oid     dbid;            /* database OID; 0 if shared map */
    Oid     tsid;            /* tablespace OID; pg_global if shared */
    int32   nbytes;          /* size of the embedded RelMapFile */
    char    data[FLEXIBLE_ARRAY_MEMBER];
} xl_relmap_update;
```

### `xl_smgr_create` (storage_xlog.h:30)

```c
typedef struct xl_smgr_create
{
    RelFileLocator  rlocator;
    ForkNumber      forkNum;
} xl_smgr_create;
```

### `xl_smgr_truncate` (storage_xlog.h:31)

```c
typedef struct xl_smgr_truncate
{
    BlockNumber     blkno;
    RelFileLocator  rlocator;
    uint32          flags;
} xl_smgr_truncate;
```

### `xl_heap_visible` (heapam_xlog.h:62)

```c
typedef struct xl_heap_visible
{
    TransactionId   cutoff_xid;
    uint8           flags;       /* VISIBILITYMAP_ALL_VISIBLE / ALL_FROZEN */
} xl_heap_visible;
```

### `xl_commit_ts_set` and `xl_commit_ts_truncate`

```c
typedef struct xl_commit_ts_set
{
    TimestampTz     timestamp;
    RepOriginId     nodeid;
    TransactionId   mainxid;
    /* TransactionId subxids[];  variable length */
} xl_commit_ts_set;

typedef struct xl_commit_ts_truncate
{
    int64           pageno;
    TransactionId   oldestXid;
} xl_commit_ts_truncate;
```

### `xl_xact_commit` (xact.h, abridged)

The full `xl_xact_commit` is built up from a sequence of optional
sub-records, each gated by an `xinfo` flag bit:

```c
typedef struct xl_xact_commit
{
    TimestampTz xact_time;
    /* xl_xact_xinfo follows if XACT_XINFO_HAS_INFO */
} xl_xact_commit;

typedef struct xl_xact_xinfo
{
    uint32 xinfo;
    /* sub-records appear conditionally:
     *    xl_xact_dbinfo
     *    xl_xact_subxacts (sub-XID array)
     *    xl_xact_relfilelocators (dropped relfilenodes)
     *    xl_xact_invals (SharedInvalidationMessage[])
     *    xl_xact_twophase
     *    xl_xact_origin
     */
} xl_xact_xinfo;
```

The `xl_xact_invals` block is what carries cache invalidation
messages from primary to standby. The `xl_xact_relfilelocators` block
carries dropped-relfilenode lists for `smgrDoPendingDeletes`.

---

[Up: index.md](index.md)  |  [Prev: appendix_glossary.md](appendix_glossary.md)  |  [Next: appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md)
