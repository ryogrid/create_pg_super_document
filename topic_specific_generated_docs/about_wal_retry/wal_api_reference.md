# PostgreSQL WAL API Reference

## Function Signatures and Common Patterns

### WAL Generation APIs

#### Primary Functions

```c
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info);
```
**Purpose**: Insert constructed WAL record into WAL buffer
**Returns**: LSN of inserted record end
**Prerequisites**: Must call `XLogBeginInsert()` first
**Thread Safety**: Thread-safe with proper insertion state

```c
void XLogBeginInsert(void);
```
**Purpose**: Initialize WAL record insertion state
**Returns**: void
**Side Effects**: Clears registered data, sets insertion flags
**Usage**: Call once before registering data/buffers

```c
void XLogRegisterData(char *data, uint32 len);
```
**Purpose**: Register main data for WAL record
**Parameters**:
- `data`: Pointer to data buffer
- `len`: Data length in bytes
**Usage**: Can be called multiple times, data chained together

```c
void XLogRegisterBuffer(int block_id, Buffer buffer, uint8 flags);
```
**Purpose**: Register buffer reference for WAL record
**Parameters**:
- `block_id`: Block identifier (0-31)
- `buffer`: Buffer reference
- `flags`: REGBUF_* flags for buffer handling

#### Internal Functions

```c
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info,
                                       XLogRecPtr RedoRecPtr, bool doPageWrites,
                                       XLogRecPtr *fpw_lsn, int *num_fpi,
                                       bool *topxid_included);
```
**Purpose**: Assemble complete WAL record from registered components
**Internal**: Not for external use
**Complexity**: Handles FPW decisions, compression, CRC calculation

```c
XLogRecPtr XLogInsertRecord(XLogRecData *rdata, XLogRecPtr fpw_lsn,
                           uint8 flags, int num_fpi, bool topxid_included);
```
**Purpose**: Low-level WAL record insertion with locking
**Internal**: Called only by `XLogInsert`
**Returns**: LSN or InvalidXLogRecPtr for retry

### WAL Writing APIs

#### Core Writing Functions

```c
void XLogFlush(XLogRecPtr record);
```
**Purpose**: Ensure WAL flushed through specified LSN
**Parameters**: `record` - LSN that must be durable
**Guarantees**: Function return = durability guaranteed
**Optimization**: Uses group commit when beneficial

```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible);
```
**Purpose**: Write WAL buffers to disk files
**Internal**: Not for direct external use
**Parameters**:
- `WriteRqst`: Write and flush positions to achieve
- `tli`: Timeline ID for file naming
- `flexible`: Allow early termination for efficiency

#### Buffer Management

```c
void AdvanceXLInsertBuffer(XLogRecPtr upto, TimeLineID tli, bool opportunistic);
```
**Purpose**: Advance WAL insertion buffer position
**Usage**: Called automatically during insertion
**Thread Safety**: Requires WAL insertion lock

```c
void WaitXLogInsertionsToFinish(XLogRecPtr upto);
```
**Purpose**: Wait for all WAL insertions to complete
**Usage**: Called before writing to ensure consistency
**Synchronization**: Uses latches for efficient waiting

### Replication APIs

#### Sender Functions

```c
static void WalSndLoop(WalSndSendDataCallback send_data);
```
**Purpose**: Main replication sender event loop
**Parameters**: `send_data` - Function pointer for data transmission
**Callbacks**: `XLogSend` (physical), `XLogSendLogical` (logical)
**Lifecycle**: Runs until connection termination

```c
void WalSndWakeup(bool physical, bool logical);
```
**Purpose**: Wake waiting WAL senders
**Parameters**:
- `physical`: Wake physical replication senders
- `logical`: Wake logical replication senders
**Usage**: Called when new WAL data available

```c
void WalSndCheckTimeOut(void);
```
**Purpose**: Check for replication timeout conditions
**Side Effects**: May terminate connection on timeout
**Configuration**: Controlled by `wal_sender_timeout`

#### Receiver Functions

```c
void WalReceiverMain(char *startup_data, size_t startup_data_len);
```
**Purpose**: Main entry point for WAL receiver process
**Parameters**: Reserved for future use (currently NULL/0)
**Lifecycle**: Runs until process termination
**Error Handling**: Reports errors and terminates for restart

```c
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len, TimeLineID tli);
```
**Purpose**: Process incoming replication messages
**Parameters**:
- `type`: Message type ('w' = WAL data, 'k' = keepalive)
- `buf`: Message payload buffer
- `len`: Buffer length
- `tli`: Timeline ID for validation

```c
void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr startptr, XLogRecPtr endptr, TimeLineID tli);
```
**Purpose**: Write received WAL data to local storage
**Parameters**: WAL data buffer with position information
**File Management**: Handles segment boundaries automatically

### Recovery APIs

#### Main Recovery Functions

```c
void StartupXLOG(void);
```
**Purpose**: Main recovery coordinator for database startup
**Usage**: Called once during postmaster startup
**Phases**: Analysis, recovery, timeline management, production transition
**Global State**: Modifies InRecovery, ControlFile state

```c
void PerformWalRecovery(void);
```
**Purpose**: Execute WAL replay loop
**Context**: Called within `StartupXLOG` during recovery phase
**Features**: Supports recovery targets, pause/resume, progress tracking
**Integration**: Coordinates with all resource managers

```c
static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI);
```
**Purpose**: Apply single WAL record during recovery
**Parameters**:
- `xlogreader`: WAL reader state
- `record`: Current record to apply
- `replayTLI`: Timeline ID (may be updated)
**Integration**: Resource manager dispatch, timeline handling

#### Recovery Support Functions

```c
XLogRecord *ReadRecord(XLogReaderState *xlogreader, int emode);
```
**Purpose**: Read next WAL record during recovery
**Returns**: Pointer to record or NULL at end
**Error Handling**: Uses specified error mode for reporting
**Buffering**: Manages WAL record buffering automatically

```c
void CheckRecoveryConsistency(void);
```
**Purpose**: Verify recovery has reached consistency
**Usage**: Called during recovery to enable connections
**Hot Standby**: Enables read-only query processing

## Common Usage Patterns

### Basic WAL Record Creation

```c
void example_wal_insert(void)
{
    XLogRecPtr lsn;
    MyWalData data = {.field1 = 123, .field2 = 456};

    XLogBeginInsert();
    XLogRegisterData((char *) &data, sizeof(MyWalData));

    // Optional: register buffers
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

    lsn = XLogInsert(RM_MY_EXTENSION_ID, XLOG_MY_OPERATION);

    // Set page LSN and ensure durability
    PageSetLSN(page, lsn);
    XLogFlush(lsn);
}
```

### Resource Manager Integration

```c
void my_extension_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_MY_OPERATION:
            {
                MyWalData *data = (MyWalData *) XLogRecGetData(record);
                // Apply the operation
                apply_my_operation(data);
                break;
            }
        default:
            elog(PANIC, "unknown my_extension redo type: %u", info);
    }
}
```

### Replication Status Monitoring

```c
void check_replication_status(void)
{
    // Get current WAL position
    XLogRecPtr current_lsn = GetXLogWriteRecPtr();

    // Check if we have active senders
    if (max_wal_senders > 0)
    {
        // Wake up any waiting senders
        WalSndWakeup(true, false);  // Physical replication
    }

    // Monitor lag (application-specific)
    XLogRecPtr last_replayed = GetXLogReplayRecPtr(NULL);
    uint64 lag_bytes = current_lsn - last_replayed;
}
```

## Data Structure Reference

### Core WAL Structures

```c
typedef struct XLogRecord
{
    uint32      xl_tot_len;     // Total record length
    TransactionId xl_xid;       // Transaction ID
    XLogRecPtr  xl_prev;        // Previous record LSN
    uint8       xl_info;        // Operation info and flags
    RmgrId      xl_rmid;        // Resource manager ID
    uint32      xl_crc;         // CRC32C checksum
    // Variable-length data follows
} XLogRecord;

typedef struct XLogRecData
{
    char       *data;           // Data pointer
    uint32      len;            // Data length in bytes
    struct XLogRecData *next;   // Next chunk in chain
} XLogRecData;

typedef struct XLogwrtRqst
{
    XLogRecPtr  Write;          // Last byte + 1 to write
    XLogRecPtr  Flush;          // Last byte + 1 to flush
} XLogwrtRqst;
```

### Replication Structures

```c
typedef struct WalSnd
{
    pid_t       pid;            // Sender process ID
    WalSndState state;          // Current state (CATCHUP/STREAMING)
    XLogRecPtr  sentPtr;        // Last position sent
    XLogRecPtr  flush;          // Last position flushed by standby
    XLogRecPtr  apply;          // Last position applied by standby
    // Additional lag tracking and sync fields
} WalSnd;

typedef struct WalRcvData
{
    pid_t       pid;            // Receiver process ID
    WalRcvState walRcvState;    // Current receiver state
    XLogRecPtr  receiveStart;   // Start position for streaming
    TimeLineID  receiveStartTLI; // Timeline for start position
    char        conninfo[MAXCONNINFO]; // Connection string
    // Additional coordination and status fields
} WalRcvData;
```

## Error Codes and Constants

### Common Error Conditions

```c
// WAL Generation Errors
#define ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE  "55000"
// "XLogBeginInsert was not called"

#define ERRCODE_INVALID_PARAMETER_VALUE           "22023"
// "invalid xlog info mask"

// Replication Errors
#define ERRCODE_CONNECTION_FAILURE               "08006"
// Connection establishment failures

#define ERRCODE_PROTOCOL_VIOLATION              "08P01"
// Invalid replication protocol messages
```

### WAL Record Info Masks

```c
#define XLR_INFO_MASK                0x0F    // Info byte mask
#define XLR_RMGR_INFO_MASK          0xF0    // Resource manager specific
#define XLR_SPECIAL_REL_UPDATE      0x01    // Special relation update
#define XLR_CHECK_CONSISTENCY       0x02    // Consistency check flag

// Buffer registration flags
#define REGBUF_FORCE_IMAGE          0x01    // Force full-page image
#define REGBUF_NO_IMAGE             0x02    // Never include image
#define REGBUF_WILL_INIT            0x04    // Page will be initialized
#define REGBUF_STANDARD             0x08    // Standard page layout
```

### State Constants

```c
// WAL Sender States
typedef enum WalSndState
{
    WALSNDSTATE_STARTUP = 0,    // Starting up
    WALSNDSTATE_BACKUP,         // Taking base backup
    WALSNDSTATE_CATCHUP,        // Sending historical WAL
    WALSNDSTATE_STREAMING,      // Real-time streaming
    WALSNDSTATE_STOPPING        // Shutting down
} WalSndState;

// Database States
#define DB_STARTUP          1       // Starting up
#define DB_SHUTDOWNED       2       // Clean shutdown
#define DB_SHUTDOWNED_IN_RECOVERY 3 // Shutdown during recovery
#define DB_SHUTDOWNING      4       // Shutting down
#define DB_IN_CRASH_RECOVERY 5      // Crash recovery
#define DB_IN_ARCHIVE_RECOVERY 6    // Archive recovery
#define DB_IN_PRODUCTION    7       // Normal operation
```

## Function Return Values

### Success/Error Patterns

```c
// XLogInsert return values
XLogRecPtr lsn = XLogInsert(rmid, info);
if (lsn == InvalidXLogRecPtr)
{
    // Only possible in bootstrap mode for non-XLOG records
    // Normal operation always returns valid LSN
}

// XLogInsertRecord return values
XLogRecPtr result = XLogInsertRecord(rdata, fpw_lsn, flags, num_fpi, topxid_included);
if (result == InvalidXLogRecPtr)
{
    // Retry required due to FPW timing change
    // Caller (XLogInsert) handles retry automatically
}
```

### Memory Management

```c
// WAL insertion uses CurrentMemoryContext
MemoryContext old_context = MemoryContextSwitchTo(CurrentMemoryContext);
// Register data and buffers
XLogRegisterData(data, len);
MemoryContextSwitchTo(old_context);

// XLogRecordAssemble uses specific insertion context
// Memory automatically cleaned up after insertion
```

## Integration Guidelines

### Adding New Resource Manager

```c
// 1. Register resource manager
static RmgrData my_rmgr_data = {
    .rm_name = "my_extension",
    .rm_redo = my_extension_redo,
    .rm_desc = my_extension_desc,
    .rm_identify = my_extension_identify,
    .rm_startup = NULL,
    .rm_cleanup = NULL,
    .rm_mask = NULL,
    .rm_decode = my_extension_decode  // For logical replication
};

void _PG_init(void)
{
    RegisterResourceManager(&my_rmgr_data);
}

// 2. Define record types
#define XLOG_MY_OPERATION    0x00
#define XLOG_MY_OTHER_OP     0x10

// 3. Implement redo function
void my_extension_redo(XLogReaderState *record);
```

### Best Practices

1. **Always call XLogBeginInsert()** before registering data
2. **Use appropriate REGBUF flags** for buffer registration
3. **Set page LSNs** after successful WAL insertion
4. **Handle retry logic** in low-level functions
5. **Validate parameters** before WAL operations
6. **Use proper error contexts** for debugging
7. **Coordinate with checkpoints** for consistency

---

**API Reference Version 1.0** | **Complete Function Signatures**

*🤖 Generated with [Claude Code](https://claude.ai/code)*