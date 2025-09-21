# PostgreSQL WAL API Reference

*Function signatures and usage patterns for PostgreSQL's Write-Ahead Logging subsystem*

---

## WAL Generation API

### Core Insertion Functions

#### XLogInsert
```c
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)
```
**Purpose**: Primary function for WAL record insertion
**Parameters**:
- `rmid`: Resource Manager ID (RM_HEAP_ID, RM_BTREE_ID, etc.)
- `info`: Operation-specific flags
**Returns**: LSN of inserted record
**Usage Pattern**:
```c
XLogBeginInsert();
XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);
XLogRegisterBuffer(buffer, REGBUF_STANDARD);
recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);
```

#### XLogInsertRecord
```c
XLogRecPtr XLogInsertRecord(XLogRecData *rdata, XLogRecPtr fpw_lsn,
                           uint8 flags, int num_fpi, bool topxid_included)
```
**Purpose**: Low-level WAL record insertion
**Parameters**:
- `rdata`: Chain of data chunks
- `fpw_lsn`: Full-page write LSN
- `flags`: Control flags
- `num_fpi`: Number of full-page images
- `topxid_included`: Transaction ID included flag
**Returns**: LSN of inserted record or InvalidXLogRecPtr on retry

#### XLogRecordAssemble
```c
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info,
                                       XLogRecPtr RedoRecPtr, bool doPageWrites,
                                       XLogRecPtr *fpw_lsn, int *num_fpi,
                                       bool *topxid_included)
```
**Purpose**: Construct complete WAL record from registered data
**Parameters**:
- `rmid`: Resource Manager ID
- `info`: Info byte with operation flags
- `RedoRecPtr`: Current redo pointer
- `doPageWrites`: Full-page writes enabled flag
- `fpw_lsn`: [OUT] Lowest LSN needing FPI
- `num_fpi`: [OUT] Count of full-page images
- `topxid_included`: [OUT] Top-level XID logged flag
**Returns**: Pointer to XLogRecData chain

### Registration Functions
```c
void XLogBeginInsert(void)
void XLogResetInsertion(void)
void XLogRegisterData(char *data, int len)
void XLogRegisterBuffer(int block_id, Buffer buffer, uint8 flags)
void XLogRegisterBufData(int block_id, char *data, int len)
```

---

## WAL Writing API

### Durability Functions

#### XLogFlush
```c
void XLogFlush(XLogRecPtr record)
```
**Purpose**: Ensure WAL data flushed to disk through specified LSN
**Parameters**:
- `record`: LSN position that must be flushed
**Usage**: Called at transaction commit and by buffer manager

#### XLogWrite
```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible)
```
**Purpose**: Write WAL data from buffers to disk
**Parameters**:
- `WriteRqst`: Write and flush positions
- `tli`: Timeline ID
- `flexible`: Allow stopping at convenient boundaries

### Position Tracking Functions
```c
XLogRecPtr GetInsertRecPtr(void)
XLogRecPtr GetFlushRecPtr(void)
XLogRecPtr GetLastImportantRecPtr(void)
void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force)
```

---

## Replication Sender API

### Main Control Functions

#### WalSndLoop
```c
static void WalSndLoop(WalSndSendDataCallback send_data)
```
**Purpose**: Main control loop for WAL sender processes
**Parameters**:
- `send_data`: Callback function for data transmission
**Usage**: Called by StartReplication and StartLogicalReplication

#### WalSndWakeup
```c
void WalSndWakeup(bool physical, bool logical)
```
**Purpose**: Wake up waiting WAL sender processes
**Parameters**:
- `physical`: Wake physical replication senders
- `logical`: Wake logical replication senders

### Communication Functions
```c
static void ProcessRepliesIfAny(void)
static void WalSndKeepaliveIfNecessary(void)
static void WalSndCheckTimeOut(void)
```

### Callback Types
```c
typedef void (*WalSndSendDataCallback)(void);
```
**Physical replication**: `XLogSendPhysical`
**Logical replication**: `XLogSendLogical`

---

## Replication Receiver API

### Main Process Functions

#### WalReceiverMain
```c
void WalReceiverMain(char *startup_data, size_t startup_data_len)
```
**Purpose**: Main entry point for WAL receiver process
**Parameters**:
- `startup_data`: Process startup data (currently unused)
- `startup_data_len`: Length of startup data

#### XLogWalRcvProcessMsg
```c
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len, TimeLineID tli)
```
**Purpose**: Process incoming replication messages
**Parameters**:
- `type`: Message type ('w' for WAL data, 'k' for keepalive)
- `buf`: Message buffer
- `len`: Message length
- `tli`: Timeline ID

#### XLogWalRcvWrite
```c
static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli)
```
**Purpose**: Write received WAL data to local storage
**Parameters**:
- `buf`: WAL data buffer
- `nbytes`: Number of bytes to write
- `recptr`: WAL record pointer
- `tli`: Timeline ID

### Interface Functions
```c
bool walrcv_connect(char *conninfo, bool logical, char *appname, char **err)
void walrcv_disconnect(void)
bool walrcv_receive(int timeout, unsigned char *type, char **buffer, int *len)
void walrcv_send(const char *buffer, int nbytes)
```

---

## Recovery API

### Main Recovery Functions

#### StartupXLOG
```c
void StartupXLOG(void)
```
**Purpose**: Main recovery coordinator function
**Usage**: Called once during database startup

#### PerformWalRecovery
```c
void PerformWalRecovery(void)
```
**Purpose**: Execute main WAL replay loop
**Usage**: Called by StartupXLOG when recovery is needed

#### ApplyWalRecord
```c
static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI)
```
**Purpose**: Apply single WAL record during recovery
**Parameters**:
- `xlogreader`: WAL reader state
- `record`: WAL record to apply
- `replayTLI`: [IN/OUT] Current replay timeline

### WAL Reading Functions
```c
XLogRecord *ReadRecord(XLogReaderState *xlogreader, int emode, bool fetching_ckpt)
bool XLogReadBufferForRedo(XLogReaderState *record, uint8 block_id, Buffer *buf)
void XLogInitBufferForRedo(XLogReaderState *record, uint8 block_id, Buffer *buf)
```

### Recovery State Functions
```c
bool CheckRecoveryConsistency(void)
void SetRecoveryPause(bool recoveryPause)
bool GetRecoveryPauseState(void)
```

---

## Concurrency Control API

### WAL Insertion Locks
```c
static void WALInsertLockAcquire(void)
static void WALInsertLockRelease(void)
static void WALInsertLockAcquireExclusive(void)
static void WALInsertLockReleaseExclusive(void)
```

### Space Reservation
```c
static XLogRecPtr ReserveXLogInsertLocation(int size)
static void CopyXLogRecordToWAL(int write_len, bool isLogSwitch,
                                XLogRecData *rdata, XLogRecPtr StartPos,
                                XLogRecPtr EndPos, TimeLineID tli)
```

### Wait Functions
```c
void WaitXLogInsertionsToFinish(XLogRecPtr upto)
```

---

## File Management API

### WAL File Operations
```c
int XLogFileInit(XLogSegNo logsegno, TimeLineID tli)
int XLogFileOpen(XLogSegNo segno, TimeLineID tli)
void XLogFileClose(void)
bool XLogArchiveCheckDone(const char *xlog)
void XLogArchiveNotify(const char *xlog)
```

### Segment Management
```c
void XLogSegNoOffsetToRecPtr(XLogSegNo segno, uint32 offset,
                             Size wal_segsz_bytes, XLogRecPtr *recptr)
void XLogRecPtrToSegNoOffset(XLogRecPtr recptr, XLogSegNo *segno, uint32 *offset,
                             Size wal_segsz_bytes)
```

---

## Timeline Management API

### Timeline Functions
```c
TimeLineID GetWALInsertionTimeLine(void)
TimeLineID GetWALInsertionTimeLineIfSet(void)
void SetWALInsertionTimeLine(TimeLineID tli)
bool tliInHistory(TimeLineID tli, List *history)
```

### Timeline History
```c
List *readTimeLineHistory(TimeLineID targetTLI)
void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI,
                          XLogRecPtr switchpoint, char *reason)
```

---

## Utility Functions

### LSN Operations
```c
char *XLogRecPtrToString(XLogRecPtr ptr)
uint32 XLogRecGetTotalLen(XLogRecord *record)
uint32 XLogRecGetDataLen(XLogRecord *record)
char *XLogRecGetData(XLogRecord *record)
```

### Compression Functions
```c
bool XLogCompressBackupBlock(char *page, uint16 hole_offset, uint16 hole_length,
                             char *dest, uint16 *dlen)
void RestoreBackupBlock(char *page, char *src, uint16 src_len,
                        uint16 hole_offset, uint16 hole_length)
```

### CRC Functions
```c
pg_crc32c XLogRecordGetCrc(XLogRecord *record)
void XLogRecordSetCrc(XLogRecord *record, pg_crc32c crc)
```

---

## Data Structure Definitions

### Core WAL Structures
```c
typedef struct XLogRecord
{
    uint32      xl_tot_len;     /* Total length of record */
    TransactionId xl_xid;       /* Transaction ID */
    XLogRecPtr  xl_prev;        /* Previous record's end */
    uint8       xl_info;        /* Info flags */
    RmgrId      xl_rmid;        /* Resource manager ID */
    pg_crc32c   xl_crc;         /* CRC32C checksum */
} XLogRecord;

typedef struct XLogRecData
{
    char       *data;           /* Data pointer */
    uint32      len;            /* Data length */
    Buffer      buffer;         /* Buffer reference */
    struct XLogRecData *next;   /* Next in chain */
} XLogRecData;
```

### Request/Result Structures
```c
typedef struct XLogwrtRqst
{
    XLogRecPtr  Write;          /* Last byte written */
    XLogRecPtr  Flush;          /* Last byte flushed */
} XLogwrtRqst;

typedef struct XLogwrtResult
{
    XLogRecPtr  Write;          /* Actual written position */
    XLogRecPtr  Flush;          /* Actual flushed position */
} XLogwrtResult;
```

---

## Common Usage Patterns

### Basic WAL Record Insertion
```c
XLogBeginInsert();
XLogRegisterData((char *) &xlrec, sizeof(xlrec));
if (BufferIsValid(buffer))
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);
PageSetLSN(page, recptr);
```

### Transaction Commit Logging
```c
XLogBeginInsert();
XLogRegisterData((char *) (&xlrec), sizeof(xlrec));
XLogRegisterData((char *) subxacts, nsubxacts * sizeof(TransactionId));
recptr = XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT);
XLogFlush(recptr);  /* Ensure durability */
```

### Recovery Pattern
```c
xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
                               XL_ROUTINE(.page_read = &XLogPageRead));
while ((record = ReadRecord(xlogreader, LOG, false)) != NULL)
{
    ApplyWalRecord(xlogreader, record, &replayTLI);
}
```

---

*This API reference covers PostgreSQL 17.6 WAL subsystem. Function signatures may vary between versions.*