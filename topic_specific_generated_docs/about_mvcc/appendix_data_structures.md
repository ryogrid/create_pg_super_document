# Appendix: Data Structures

> MVCC Documentation > Appendix > Data Structures

---

This appendix documents the key struct definitions used by the MVCC system with field-level descriptions.

## HeapTupleHeaderData

The 23-byte fixed header for every heap tuple. Defined at `src/include/access/htup_details.h:153`.

```c
struct HeapTupleHeaderData
{
    union
    {
        HeapTupleFields t_heap;      /* Transaction visibility fields */
        DatumTupleFields t_datum;    /* In-memory composite type fields */
    }           t_choice;

    ItemPointerData t_ctid;          /* Current TID of this or newer tuple
                                      * (or a speculative insertion token) */

    uint16      t_infomask2;         /* Number of attributes + flags */
    uint16      t_infomask;          /* Various flag bits */
    uint8       t_hoff;              /* Sizeof header incl. bitmap, padding */

    /* ^ - 23 bytes - ^ */

    bits8       t_bits[FLEXIBLE_ARRAY_MEMBER]; /* Bitmap of NULLs */
};
```

| Field | Size | Description |
|-------|------|-------------|
| `t_choice.t_heap` | 12 bytes | Transaction fields (t_xmin, t_xmax, t_cid/t_xvac). Active when tuple is on disk. |
| `t_choice.t_datum` | 12 bytes | In-memory fields. Active only when tuple is a composite datum. |
| `t_ctid` | 6 bytes | Self-TID if latest version; points to newer version if updated; speculative token for ON CONFLICT. |
| `t_infomask2` | 2 bytes | Lower 11 bits: attribute count. Upper bits: HOT_UPDATED, HEAP_ONLY_TUPLE, KEYS_UPDATED. |
| `t_infomask` | 2 bytes | Hint bits, lock state, TOAST/null flags. See [Tuple Versioning: Infomask Reference](04_tuple_versioning.md). |
| `t_hoff` | 1 byte | Offset to user data (includes null bitmap and alignment padding). |
| `t_bits` | variable | Null bitmap; one bit per attribute. Present only if HEAP_HASNULL is set. |

**See:** [Tuple Versioning](04_tuple_versioning.md) for complete infomask flag tables.

---

## HeapTupleFields

Transaction-related fields within the tuple header union. Defined at `src/include/access/htup_details.h:122`.

```c
typedef struct HeapTupleFields
{
    TransactionId t_xmin;        /* inserting xact ID */
    TransactionId t_xmax;        /* deleting or locking xact ID */

    union
    {
        CommandId   t_cid;       /* inserting or deleting command ID, or both */
        TransactionId t_xvac;   /* old-style VACUUM FULL xact ID */
    }           t_field3;
} HeapTupleFields;
```

| Field | Size | Description |
|-------|------|-------------|
| `t_xmin` | 4 bytes | XID of the transaction that created this tuple version. Set by [heap_insert()](04_tuple_versioning.md) and [heap_update()](04_tuple_versioning.md). |
| `t_xmax` | 4 bytes | XID of the transaction that deleted/updated/locked this tuple. 0 (`InvalidTransactionId`) if not deleted. May be a MultiXactId if `HEAP_XMAX_IS_MULTI` is set. |
| `t_field3.t_cid` | 4 bytes | Command ID for same-transaction [visibility checks](05_visibility_rules.md). May be a combo CID if HEAP_COMBOCID is set. |
| `t_field3.t_xvac` | 4 bytes | XID of pre-9.0 VACUUM FULL that moved the tuple. Legacy; shares storage with t_cid. |

**Five logical values in three physical fields:** xmin, xmax, cmin, cmax, and xvac. The cmin/cmax sharing is handled by combo CIDs. See [Tuple Versioning: ComboCID](04_tuple_versioning.md).

---

## SnapshotData

The MVCC snapshot structure. Defined at `src/include/utils/snapshot.h:142`.

```c
typedef struct SnapshotData
{
    SnapshotType snapshot_type;         /* type of snapshot */

    TransactionId xmin;                 /* all XID < xmin are visible to me */
    TransactionId xmax;                 /* all XID >= xmax are invisible to me */

    TransactionId *xip;                 /* in-progress xact IDs */
    uint32      xcnt;                   /* # of xact ids in xip[] */

    TransactionId *subxip;              /* in-progress subtxn XIDs */
    int32       subxcnt;                /* # of xact ids in subxip[] */
    bool        suboverflowed;          /* has the subxip array overflowed? */

    bool        takenDuringRecovery;    /* recovery-shaped snapshot? */
    bool        copied;                 /* false if static snapshot */

    CommandId   curcid;                 /* in my xact, CID < curcid are visible */

    uint32      speculativeToken;       /* for SNAPSHOT_DIRTY */
    struct GlobalVisState *vistest;     /* for SNAPSHOT_NON_VACUUMABLE */

    uint32      active_count;           /* refcount on ActiveSnapshot stack */
    uint32      regd_count;             /* refcount on RegisteredSnapshots */
    pairingheap_node ph_node;           /* link in RegisteredSnapshots heap */

    TimestampTz whenTaken;              /* timestamp when snapshot was taken */
    XLogRecPtr  lsn;                    /* WAL position when taken */

    uint64      snapXactCompletionCount; /* for snapshot reuse optimization */
} SnapshotData;
```

| Field | Description |
|-------|-------------|
| `snapshot_type` | Determines which [visibility function](05_visibility_rules.md) is used (MVCC, SELF, DIRTY, etc.). |
| `xmin` | Lower bound. XIDs below this are definitely finished. Fast-path in [XidInMVCCSnapshot()](05_visibility_rules.md). |
| `xmax` | Upper bound (always `latestCompletedXid + 1`). XIDs at or above this are in-progress. |
| `xip[]` / `xcnt` | In-progress top-level XIDs. Searched by [XidInMVCCSnapshot()](05_visibility_rules.md). |
| `subxip[]` / `subxcnt` | In-progress subtransaction XIDs. |
| `suboverflowed` | If true, subxip[] is incomplete; forces pg_subtrans fallback. |
| `curcid` | Same-transaction visibility: tuples with cmin >= curcid are invisible. |
| `snapXactCompletionCount` | Enables [snapshot reuse](06_snapshot_management.md) when no transactions have completed since last snapshot. |

**See:** [Snapshot Management](06_snapshot_management.md) for complete lifecycle details.

---

## PGPROC

Per-backend shared memory structure. Defined at `src/include/storage/proc.h:162`. Only MVCC-relevant fields shown.

```c
struct PGPROC
{
    /* Transaction identification */
    TransactionId xid;              /* top-level XID, mirrored in ProcGlobal->xids[] */
    TransactionId xmin;             /* oldest XID needed by this backend */
    int           pgxactoff;        /* index into ProcGlobal dense arrays */

    /* Virtual transaction ID */
    struct {
        ProcNumber  procNumber;
        LocalTransactionId lxid;
    } vxid;

    /* Status */
    uint8         statusFlags;      /* PROC_IN_VACUUM, etc., mirrored in ProcGlobal */

    /* Subtransaction cache */
    XidCacheStatus subxidStatus;    /* {count, overflowed}, mirrored */
    struct XidCache {
        TransactionId xids[PGPROC_MAX_CACHED_SUBXIDS]; /* cached subxact XIDs (64 max) */
    } subxids;

    /* Group XID clearing */
    bool          procArrayGroupMember;
    pg_atomic_uint32 procArrayGroupNext;
    TransactionId procArrayGroupMemberXid;

    /* Group CLOG update */
    bool          clogGroupMember;
    pg_atomic_uint32 clogGroupNext;
    TransactionId clogGroupMemberXid;
    XidStatus     clogGroupMemberXidStatus;
    int64         clogGroupMemberPage;
    XLogRecPtr    clogGroupMemberLsn;

    /* Checkpoint delay coordination */
    int           delayChkptFlags;  /* DELAY_CHKPT_START, DELAY_CHKPT_COMPLETE */
};
```

| Field | Description |
|-------|-------------|
| `xid` | Current top-level XID. Set by [GetNewTransactionId()](03_transaction_lifecycle.md), cleared by [ProcArrayEndTransaction()](07_concurrency_infrastructure.md). |
| `xmin` | Oldest XID this backend might access. Set during [GetSnapshotData()](06_snapshot_management.md). Prevents [VACUUM](09_vacuum_and_freezing.md) from removing needed tuples. |
| `pgxactoff` | Offset into the dense arrays. Valid only under ProcArrayLock or XidGenLock. |
| `subxidStatus` | Count of cached subtransaction XIDs and overflow flag. When overflowed, snapshots fall back to pg_subtrans. |
| `procArrayGroupMember*` | Fields for [group clearing](07_concurrency_infrastructure.md) optimization. |
| `clogGroupMember*` | Fields for group [CLOG](08_clog_transaction_status.md) update optimization. |

**See:** [Concurrency Infrastructure](07_concurrency_infrastructure.md).

---

## PROC_HDR (ProcGlobal)

Global process header with dense arrays. Defined at `src/include/storage/proc.h:370`.

```c
typedef struct PROC_HDR
{
    PGPROC     *allProcs;               /* Array of all PGPROC structures */
    TransactionId *xids;                /* Dense mirror of PGPROC.xid */
    XidCacheStatus *subxidStates;       /* Dense mirror of PGPROC.subxidStatus */
    uint8      *statusFlags;            /* Dense mirror of PGPROC.statusFlags */
    uint32      allProcCount;           /* Length of allProcs array */

    pg_atomic_uint32 procArrayGroupFirst; /* Group clearing linked list head */
    pg_atomic_uint32 clogGroupFirst;      /* Group CLOG update linked list head */
} PROC_HDR;
```

**See:** [Concurrency Infrastructure](07_concurrency_infrastructure.md), [diagrams/shared_memory_layout.mermaid](diagrams/shared_memory_layout.mermaid).

---

## VacuumCutoffs

Freeze cutoff structure for VACUUM. Defined in `src/include/commands/vacuum.h`.

```c
struct VacuumCutoffs
{
    TransactionId relfrozenxid;    /* Current pg_class.relfrozenxid */
    MultiXactId relminmxid;        /* Current pg_class.relminmxid */
    TransactionId OldestXmin;      /* Dead-tuple removal horizon */
    MultiXactId OldestMxact;       /* MultiXact removal horizon */
    TransactionId FreezeLimit;     /* XIDs below this must be frozen (aggressive) */
    MultiXactId MultiXactCutoff;   /* MXIDs below this must be frozen (aggressive) */
};
```

| Field | Description |
|-------|-------------|
| `relfrozenxid` | Current frozen XID horizon for this relation. |
| `OldestXmin` | Oldest XID any backend needs. Tuples with xmax < OldestXmin are DEAD. |
| `FreezeLimit` | `nextXID - vacuum_freeze_min_age`. Below this, xmin can be frozen. |

**See:** [VACUUM and Freezing](09_vacuum_and_freezing.md).

---

## HeapTupleFreeze

Per-tuple freeze plan. Defined in `src/include/access/heapam.h`.

```c
typedef struct HeapTupleFreeze
{
    TransactionId xmax;            /* New xmax value */
    uint16      t_infomask2;       /* New infomask2 */
    uint16      t_infomask;        /* New infomask */
    uint8       frzflags;          /* Freeze action flags */
    uint8       checkflags;        /* Verification flags */
    OffsetNumber offset;           /* Tuple's offset on the page */
} HeapTupleFreeze;
```

**See:** [VACUUM and Freezing: heap_prepare_freeze_tuple](09_vacuum_and_freezing.md).

---

## HeapPageFreeze

Page-level freeze tracking. Defined in `src/include/access/heapam.h`.

```c
typedef struct HeapPageFreeze
{
    bool        freeze_required;          /* Must freeze this page? */
    TransactionId FreezePageRelfrozenXid; /* Oldest XID if page IS frozen */
    TransactionId NoFreezePageRelfrozenXid; /* Oldest XID if page is NOT frozen */
    MultiXactId FreezePageRelminMxid;     /* Oldest MXID if page IS frozen */
    MultiXactId NoFreezePageRelminMxid;   /* Oldest MXID if page is NOT frozen */
} HeapPageFreeze;
```

The dual tracking (Freeze vs NoFreeze variants) allows the freeze decision to be made after all tuples are analyzed.

**See:** [VACUUM and Freezing](09_vacuum_and_freezing.md).

---

## LVRelState

Central state structure for lazy VACUUM. Defined at `src/backend/access/heap/vacuumlazy.c:136`.

```c
typedef struct LVRelState
{
    Relation    rel;                /* Target heap relation */
    Relation   *indrels;            /* Index relations */
    int         nindexes;           /* Number of indexes */

    bool        aggressive;         /* Must advance relfrozenxid? */
    bool        do_index_vacuuming; /* Performing index vacuuming? */
    bool        do_rel_truncate;    /* Truncating relation? */

    struct VacuumCutoffs cutoffs;   /* Freeze/prune cutoffs */
    GlobalVisState *vistest;        /* Dead-tuple visibility test */
    TransactionId NewRelfrozenXid;  /* Tracking oldest unfrozen XID */
    MultiXactId NewRelminMxid;      /* Tracking oldest unfrozen MXID */

    TidStore   *dead_items;         /* Dead item TIDs for index cleanup */

    /* Counters */
    int64       tuples_deleted;
    int64       tuples_frozen;
    int64       lpdead_items;
    int64       live_tuples;
    int64       recently_dead_tuples;
    int64       missed_dead_tuples;
} LVRelState;
```

**See:** [VACUUM and Freezing](09_vacuum_and_freezing.md).

---

Previous: [Appendix: Glossary](appendix_glossary.md) | Next: [Quick Reference](mvcc_quick_reference.md)
