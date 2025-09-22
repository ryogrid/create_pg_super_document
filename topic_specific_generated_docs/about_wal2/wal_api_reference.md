# PostgreSQL WAL API Cheat Sheet

## Function Signatures & Common Patterns

### WAL Generation Functions

```c
// Main entry point - call after XLogBeginInsert() and XLogRegister*()
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info);

// Low-level insertion - typically called by XLogInsert
XLogRecPtr XLogInsertRecord(XLogRecData *rdata, XLogRecPtr fpw_lsn,
                           uint8 flags, int num_fpi, bool topxid_included);

// Assembly - builds complete record structure
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info,
                                      XLogRecPtr RedoRecPtr, bool doPageWrites,
                                      XLogRecPtr *fpw_lsn, int *num_fpi,
                                      bool *topxid_included);

// Concurrency control
static void WALInsertLockAcquire(void);
static void WALInsertLockRelease(void);
```

### WAL Writing Functions

```c
// Force to persistent storage
void XLogFlush(XLogRecPtr record);

// Batch write to disk files
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible);

// Get FPW settings
void GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p);

// Update write tracking
static void RefreshXLogWriteResult(XLogwrtRqst WriteRqst);
```

### Replication Sender Functions

```c
// Main sender loop
static void WalSndLoop(WalSndSendDataCallback send_data);

// Physical replication data transmission
static void XLogSendPhysical(void);

// Process standby replies
static void ProcessRepliesIfAny(void);

// Heartbeat mechanism
static void WalSndKeepalive(bool requestReply, XLogRecPtr writePtr);

// Wait for WAL availability
static XLogRecPtr WalSndWaitForWal(XLogRecPtr loc);
```

### Replication Receiver Functions

```c
// Main receiver entry point
void WalReceiverMain(char *startup_data, size_t startup_data_len);

// Process incoming messages
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len, TimeLineID tli);

// Write received data
static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli);

// Flush to disk
static void XLogWalRcvFlush(bool dying, TimeLineID tli);

// Send progress feedback
static void XLogWalRcvSendReply(bool force, bool requestReply);
```

### Recovery Functions

```c
// Main recovery entry point
void StartupXLOG(void);

// Core recovery loop
void PerformWalRecovery(void);

// Apply individual records
static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI);

// Resource manager dispatch
extern RmgrData RmgrTable[RM_MAX_ID + 1];

// Record reading
XLogRecord *ReadRecord(XLogReaderState *xlogreader, int emode, bool fetching_ckpt, TimeLineID replayTLI);
```

### Synchronous Replication Functions

```c
// Wait for LSN acknowledgment
void SyncRepWaitForLSN(XLogRecPtr lsn, bool commit);

// Process standby reply
static void ProcessStandbyReplyMessage(void);
```

## Common Usage Patterns

### Basic WAL Insertion
```c
// 1. Begin insertion
XLogBeginInsert();

// 2. Register data/buffers
XLogRegisterData((char *) &data, sizeof(data));
XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

// 3. Insert record
lsn = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);

// 4. Clean up
XLogResetInsertion();
```

### Resource Manager Implementation
```c
// Define RMGR entry
const RmgrData heap_desc = {
    .rm_name = "Heap",
    .rm_redo = heap_redo,
    .rm_desc = heap_desc,
    .rm_identify = heap_identify,
    .rm_startup = NULL,
    .rm_cleanup = NULL,
    .rm_mask = NULL,
    .rm_decode = heap_decode
};

// Redo function implementation
void heap_redo(XLogReaderState *record) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
            heap_xlog_insert(record);
            break;
        case XLOG_HEAP_DELETE:
            heap_xlog_delete(record);
            break;
        // ... other cases
    }
}
```

### Standby Message Processing
```c
void ProcessStandbyReplyMessage(void) {
    XLogRecPtr writePtr, flushPtr, applyPtr;
    bool replyRequested;

    // Parse message
    writePtr = pq_getmsgint64(&incoming_message);
    flushPtr = pq_getmsgint64(&incoming_message);
    applyPtr = pq_getmsgint64(&incoming_message);

    // Update progress tracking
    WalSnd->write = writePtr;
    WalSnd->flush = flushPtr;
    WalSnd->apply = applyPtr;

    // Wake up waiting transactions
    SyncRepReleaseWaiters();
}
```

## Data Structure Quick Reference

### Core Structures
```c
// WAL record header
typedef struct XLogRecord {
    uint32      xl_tot_len;     // Total length
    TransactionId xl_xid;       // Transaction ID
    XLogRecPtr  xl_prev;        // Previous record LSN
    uint8       xl_info;        // Info flags
    RmgrId      xl_rmid;        // Resource manager ID
    pg_crc32c   xl_crc;         // Checksum
} XLogRecord;

// Data chain for record assembly
typedef struct XLogRecData {
    struct XLogRecData *next;   // Next chunk
    char       *data;           // Data pointer
    uint32      len;            // Length
} XLogRecData;

// Write request
typedef struct XLogwrtRqst {
    XLogRecPtr  Write;          // Write position
    XLogRecPtr  Flush;          // Flush position
} XLogwrtRqst;

// WAL sender state
typedef struct WalSnd {
    pid_t       pid;            // Process ID
    WalSndState state;          // Current state
    XLogRecPtr  sentPtr;        // Last sent LSN
    XLogRecPtr  flush;          // Last flushed LSN
    XLogRecPtr  apply;          // Last applied LSN
    TimestampTz replyTime;      // Last reply time
} WalSnd;
```

## Error Codes & Return Values

### Return Value Conventions
- **XLogRecPtr**: Valid LSN on success, InvalidXLogRecPtr on failure
- **void**: Functions that always succeed or PANIC on error
- **bool**: Success/failure indicator
- **static**: Internal functions, not part of public API

### Common Error Scenarios
```c
// XLogInsert validation
if (info & ~XLR_RMGR_INFO_MASK)
    elog(PANIC, "invalid info mask");

// XLogInsertRecord retry scenario
if (fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= RedoRecPtr)
    return InvalidXLogRecPtr;  // Caller will retry

// WAL reading error
if (record == NULL)
    ereport(emode, "could not read WAL record");
```

## Performance Constants

### Key Limits
```c
#define XLOG_BLCKSZ 8192           // WAL block size
#define XLOG_SEG_SIZE (16*1024*1024) // Default segment size
#define NUM_XLOGINSERT_LOCKS 8     // Insertion lock count
#define MAX_BACKENDS 262144        // Maximum connections
```

### Buffer Sizes
```c
// Default WAL buffer size calculation
wal_buffers = min(segment_size, shared_buffers / 32);

// Minimum effective values
min_wal_buffers = 64 * 1024;      // 64KB
max_wal_buffers = 2048 * 1024;    // 2MB default max
```

*This cheat sheet covers the 30 most critical WAL functions with their signatures and usage patterns.*