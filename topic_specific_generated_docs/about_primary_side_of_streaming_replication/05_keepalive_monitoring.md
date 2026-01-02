# Chapter 5: Keepalive and Monitoring

<- [Previous: Walsender Transmission](04_walsender_transmission.md) | [Index](index.md) | [Next: Standby Response Processing](06_standby_response.md) ->

---

## Overview

This chapter covers the keepalive mechanism and timeout monitoring in walsender. These mechanisms ensure:

- Detection of unresponsive standbys
- Prompt synchronous replication confirmation
- Graceful connection termination on failure

The key functions are `WalSndKeepalive()`, `WalSndKeepaliveIfNecessary()`, and `WalSndCheckTimeOut()`.

---

## Processing Flow

The keepalive and monitoring flow:

```
WalSndLoop iteration
    |
    +---> WalSndCheckTimeOut()
    |         |
    |         +---> If full timeout elapsed: WalSndShutdown()
    |
    +---> WalSndKeepaliveIfNecessary()
              |
              +---> If half timeout elapsed: WalSndKeepalive(true)
              |         |
              |         +---> Standby replies with positions
              |
              +---> last_reply_timestamp updated
```

---

## Implementation Details

### WalSndKeepalive Function

**Location:** `src/backend/replication/walsender.c` (approximately line 3600)

**Signature:**
```c
static void WalSndKeepalive(bool requestReply, XLogRecPtr writePtr)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `requestReply` | bool | Whether to request a reply from standby |
| `writePtr` | XLogRecPtr | WAL position to include (or InvalidXLogRecPtr) |

#### Keepalive Message Format

The keepalive message ('k') format:

| Field | Size | Description |
|-------|------|-------------|
| msgtype | 1 byte | 'k' for keepalive |
| walEnd | 8 bytes | Current WAL end position |
| sendTime | 8 bytes | Server timestamp |
| replyRequested | 1 byte | 1 if reply requested, 0 otherwise |

#### Implementation

```c
static void
WalSndKeepalive(bool requestReply, XLogRecPtr writePtr)
{
    /* Get current WAL position if not provided */
    if (XLogRecPtrIsInvalid(writePtr))
        writePtr = GetFlushRecPtr(NULL);

    /* Build keepalive message */
    resetStringInfo(&output_message);
    pq_sendbyte(&output_message, 'k');           /* keepalive message */
    pq_sendint64(&output_message, writePtr);     /* current WAL position */
    pq_sendint64(&output_message, GetCurrentTimestamp()); /* timestamp */
    pq_sendbyte(&output_message, requestReply);  /* reply requested? */

    /* Send it */
    pq_putmessage_noblock('d', output_message.data, output_message.len);

    /* Track that we're waiting for a response */
    if (requestReply)
        waiting_for_ping_response = true;
}
```

---

### WalSndKeepaliveIfNecessary Function

**Location:** `src/backend/replication/walsender.c` (approximately line 3737)

Called from `WalSndLoop()` to send keepalive at appropriate intervals:

```c
static void
WalSndKeepaliveIfNecessary(void)
{
    TimestampTz now;
    TimestampTz ping_time;

    /* Don't send if we don't have a timeout configured */
    if (wal_sender_timeout <= 0)
        return;

    /* Don't send if already waiting for a response */
    if (waiting_for_ping_response)
        return;

    now = GetCurrentTimestamp();

    /* Send keepalive if half of timeout elapsed since last reply */
    ping_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
                                            wal_sender_timeout / 2);

    if (now >= ping_time)
    {
        WalSndKeepalive(true, InvalidXLogRecPtr);

        /* Update ping time for timeout calculation */
        last_reply_timestamp = now;
    }
}
```

#### Timing Logic

```
Timeline:
|------ wal_sender_timeout (60s) ------|
|-- half (30s) --|-- half (30s) --|
                 ^                 ^
                 |                 |
            send             timeout
          keepalive           check
```

By sending keepalive at the halfway point, there is sufficient time for:
1. Keepalive transmission to standby
2. Standby processing
3. Reply transmission back to primary

---

### WalSndCheckTimeOut Function

**Location:** `src/backend/replication/walsender.c:2758`

```c
static void
WalSndCheckTimeOut(void)
{
    TimestampTz timeout;
    TimestampTz now;

    /* Skip if no timeout configured */
    if (wal_sender_timeout <= 0)
        return;

    now = GetCurrentTimestamp();

    /* Calculate when timeout should occur */
    timeout = TimestampTzPlusMilliseconds(last_reply_timestamp,
                                          wal_sender_timeout);

    if (now >= timeout)
    {
        /*
         * Since typically expiration of replication timeout means
         * communication problem, we don't send the error message to
         * the standby.
         */
        ereport(COMMERROR,
                (errmsg("terminating walsender process due to replication timeout")));

        WalSndShutdown();
    }
}
```

---

### Reply Timestamp Tracking

The `last_reply_timestamp` variable is updated in multiple places:

| Location | When Updated |
|----------|--------------|
| `WalSndLoop()` initialization | At loop start |
| `ProcessRepliesIfAny()` | When any message received from standby |
| `WalSndKeepaliveIfNecessary()` | When sending keepalive (resets timeout window) |

```c
// In ProcessRepliesIfAny() after processing messages
if (received)
{
    last_reply_timestamp = GetCurrentTimestamp();
    waiting_for_ping_response = false;
}
```

---

### Standby Reply Triggering

The standby sends replies in these situations:

| Trigger | Configuration | Description |
|---------|---------------|-------------|
| Periodic status | `wal_receiver_status_interval` (default 10s) | Regular status updates |
| Keepalive response | `replyRequested = true` | Immediate reply to keepalive |
| Position updates | Internal | When significant progress made |

#### Walreceiver Reply Message Format

The standby reply message ('r') format:

| Field | Size | Description |
|-------|------|-------------|
| msgtype | 1 byte | 'r' for reply |
| write | 8 bytes | Last position written to disk |
| flush | 8 bytes | Last position flushed to disk |
| apply | 8 bytes | Last position applied/replayed |
| replyTime | 8 bytes | Standby timestamp |
| replyRequested | 1 byte | 1 if standby requests a reply |

**Cross-reference:** See [Chapter 6](06_standby_response.md#processstandbyreplymessage-function) for how this message is processed.

---

### Error Handling

When timeout occurs:

```c
static void
WalSndShutdown(void)
{
    /*
     * Reset whereToSendOutput to prevent ereport from attempting
     * to send any more messages to the standby.
     */
    if (whereToSendOutput == DestRemote)
        whereToSendOutput = DestNone;

    proc_exit(0);
}
```

The process exits cleanly, and the standby's walreceiver will detect the disconnection via its own timeout (`wal_receiver_timeout`).

---

## Interaction with Synchronous Replication

Keepalives play an important role in synchronous replication:

1. **Triggering sync rep confirmation:** A keepalive with `requestReply=true` causes the standby to immediately reply with its current positions. This allows [SyncRepReleaseWaiters()](07_sync_wait_release.md#syncrepreleasewaiters-function) to run promptly.

2. **Ensuring timely release:** Without keepalives, sync rep waiters might wait longer than necessary if no new WAL is being generated.

3. **Detecting failed standbys:** Timeout detection prevents infinite waits for synchronous replication confirmation.

### Keepalive in Response Processing

```c
// In ProcessStandbyReplyMessage()
if (replyRequested)
    WalSndKeepalive(false, InvalidXLogRecPtr);
```

If the standby requests a reply (unusual), the walsender responds with a keepalive (without requesting reply back).

---

## Timing Diagram

```
Primary                                 Standby
   |                                       |
   | ---- WAL data ------------------->    |
   |                                       |
   | (wal_receiver_status_interval)        |
   |                                       |
   | <---- Reply (write/flush/apply) ----  |
   |                                       |
   | last_reply_timestamp updated          |
   |                                       |
   | (half of wal_sender_timeout)          |
   |                                       |
   | ---- Keepalive (reply requested) ->   |
   |                                       |
   | <---- Reply immediately -----------   |
   |                                       |
   | last_reply_timestamp updated          |
   |                                       |
```

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `wal_sender_timeout` | 60s | Time before walsender terminates unresponsive standby. Keepalive sent at half this interval. |
| `wal_receiver_timeout` | 60s | Time before walreceiver terminates unresponsive primary. |
| `wal_receiver_status_interval` | 10s | How often walreceiver sends status (reply) messages. Lower values improve sync rep responsiveness. |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete documentation.

---

## Key Takeaways

1. **Half-timeout keepalive:** Keepalives are sent at half the `wal_sender_timeout` interval (default: 30s), providing time for round-trip before full timeout.

2. **Reply requested flag:** Setting `replyRequested = true` triggers immediate standby response, essential for timely sync rep confirmation.

3. **Clean termination:** Timeout causes clean process termination via `proc_exit(0)`. The standby detects disconnection via its own timeout.

4. **last_reply_timestamp tracking:** Updated on any message from standby. Represents communication health.

5. **No infinite waits:** The mechanism ensures synchronous replication doesn't wait indefinitely for a failed or partitioned standby.

6. **Network issue detection:** Network issues are detected within `wal_sender_timeout` seconds (default: 60s).

7. **Sync rep interaction:** Keepalives can trigger sync rep confirmation even when no new WAL is being generated.

---

## Related Sections

- **Previous:** [Chapter 4: Walsender Transmission](04_walsender_transmission.md) - Main loop context
- **Next:** [Chapter 6: Standby Response Processing](06_standby_response.md) - How replies are processed
- **Sync Rep:** [Chapter 7: Sync Wait/Release](07_sync_wait_release.md) - How waiters are released

---

## Navigation

<- [Previous: Walsender Transmission](04_walsender_transmission.md) | [Index](index.md) | [Next: Standby Response Processing](06_standby_response.md) ->
