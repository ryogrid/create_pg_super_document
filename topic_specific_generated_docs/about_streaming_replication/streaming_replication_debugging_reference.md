# PostgreSQL Streaming Replication Debugging Reference

## Overview

This comprehensive debugging reference provides implementation-specific troubleshooting techniques for PostgreSQL's streaming replication system. It focuses on practical diagnostic approaches, error interpretation, and performance bottleneck identification based on detailed analysis of the streaming replication implementation.

## Relationship to Existing Documentation

> **Foundation**: This debugging guide complements the conceptual foundation provided in:
> - [WAL Complete Documentation](topic_specific_generated_docs/about_wal/wal_complete_documentation.md)
> - [Replication Sender Component](topic_specific_generated_docs/about_wal/component_replication_sender.md)
> - [Replication Receiver Component](topic_specific_generated_docs/about_wal/component_replication_receiver.md)
> - [Recovery Component](topic_specific_generated_docs/about_wal/component_recovery.md)

**What This Guide Adds**:
- Implementation-specific debugging techniques
- Performance bottleneck identification based on source code analysis
- Error code interpretation with function-level context
- Log analysis techniques for streaming replication components
- Network and memory debugging approaches

## Quick Diagnostic Index

### Common Issues by Symptom
- **[Replication Lag](#replication-lag-diagnosis)** - Standby falling behind primary
- **[Connection Failures](#connection-debugging)** - WalReceiver unable to connect
- **[Process Crashes](#process-crash-analysis)** - WalSender/WalReceiver unexpected exits
- **[Performance Issues](#performance-bottleneck-identification)** - High latency or throughput problems
- **[Slot Problems](#replication-slot-debugging)** - Slot creation or management failures
- **[Timeline Issues](#timeline-debugging)** - Timeline switching problems

### By Component
- **[Primary Side](#primary-side-debugging)** - WalSender and WAL generation issues
- **[Standby Side](#standby-side-debugging)** - WalReceiver and startup process problems
- **[Network Layer](#network-debugging)** - Protocol and connection issues
- **[Shared Memory](#shared-memory-debugging)** - Inter-process coordination problems

## Replication Lag Diagnosis

### 1. Lag Measurement and Analysis

#### Primary Lag Tracking Implementation
```sql
-- Check current lag using system functions
SELECT
    client_addr,
    application_name,
    state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) AS flush_lag_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes,
    write_lag,
    flush_lag,
    replay_lag
FROM pg_stat_replication;
```

#### Implementation-Level Lag Sources

**Function**: `LagTrackerWrite()` in `src/backend/replication/syncrep.c:487-542`

**Key Lag Tracking Points**:
1. **Write Lag**: Time from WAL write to standby acknowledgment
2. **Flush Lag**: Time from WAL flush to standby flush confirmation
3. **Replay Lag**: Time from primary commit to standby replay completion

**Diagnostic Commands**:
```sql
-- Check WAL sender buffer state
SELECT pid, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
       pg_wal_lsn_diff(sent_lsn, write_lsn) as pending_write_bytes,
       pg_wal_lsn_diff(write_lsn, flush_lsn) as pending_flush_bytes
FROM pg_stat_replication;

-- Check WAL generation rate
SELECT
    pg_wal_lsn_diff(pg_current_wal_lsn(),
                    lag(pg_current_wal_lsn()) OVER (ORDER BY now())) /
    EXTRACT(epoch FROM (now() - lag(now()) OVER (ORDER BY now()))) as wal_bytes_per_second
FROM pg_stat_replication CROSS JOIN (SELECT now()) t(now);
```

#### Root Cause Analysis

**Primary Side Bottlenecks**:
1. **WAL Insertion Lock Contention**: Monitor `XLogInsertRecord()` performance
   ```sql
   -- Check WAL insertion contention
   SELECT wait_event_type, wait_event, count(*)
   FROM pg_stat_activity
   WHERE wait_event LIKE '%WAL%'
   GROUP BY wait_event_type, wait_event;
   ```

2. **WalSender Wakeup Latency**: Check `WalSndWakeup()` call frequency
   ```sql
   -- Monitor WalSender process activity
   SELECT pid, state, query_start, wait_event_type, wait_event
   FROM pg_stat_activity
   WHERE backend_type = 'walsender';
   ```

**Standby Side Bottlenecks**:
1. **Startup Process Replay Speed**: Monitor `XLogReadRecord()` performance
2. **WalReceiver Write Performance**: Check `XLogWalRcvWrite()` call duration

### 2. Performance Bottleneck Identification

#### Network Transmission Bottlenecks

**Function**: `WalSndLoop()` in `src/backend/replication/walsender.c:2784-2923`

**Key Diagnostic Points**:
```c
// Check for network congestion in WalSender
if (pq_is_send_pending()) {
    // Network buffer is full - indicates congestion
    WalSndCaughtUp = false;
}
```

**Monitoring Techniques**:
```bash
# Check network buffer utilization
ss -i | grep ":5432"  # PostgreSQL port
netstat -i            # Interface statistics

# Monitor TCP congestion
ss -i | grep cwnd      # Congestion window size
```

#### Storage I/O Bottlenecks

**Primary Side - WAL Writing**:
```sql
-- Check WAL writing statistics
SELECT
    wal_records,
    wal_fpi,
    wal_bytes,
    wal_buffers_full,
    wal_write,
    wal_sync,
    wal_write_time,
    wal_sync_time
FROM pg_stat_wal;
```

**Standby Side - WAL Reception**:
```sql
-- Monitor WAL receiver statistics
SELECT
    pg_stat_get_wal_receiver_pid() as walrcv_pid,
    pg_stat_get_wal_senders() as active_senders;
```

## Connection Debugging

### 1. WalReceiver Connection Establishment

#### Connection Process Analysis

**Function**: `walrcv_connect()` in `src/backend/replication/libpqwalreceiver.c:86-158`

**Connection Failure Points**:
1. **DNS Resolution**: Check hostname resolution
2. **Authentication**: Verify replication user permissions
3. **SSL/TLS**: Certificate validation issues
4. **Network Connectivity**: Firewall or routing problems

**Diagnostic Approaches**:
```bash
# Test basic connectivity
psql "host=primary_host port=5432 user=replicator replication=1" -c "IDENTIFY_SYSTEM;"

# Check SSL configuration
psql "host=primary_host port=5432 user=replicator replication=1 sslmode=require" \
     -c "SELECT version();"

# Verify replication permissions
psql -h primary_host -U replicator -c "SELECT rolreplication FROM pg_roles WHERE rolname = 'replicator';"
```

#### Connection State Monitoring

**Shared Memory State**: `WalRcvData->walRcvState`

**State Values**:
- `WALRCV_STOPPED`: Not running
- `WALRCV_STARTING`: Initialization in progress
- `WALRCV_STREAMING`: Active streaming
- `WALRCV_WAITING`: Waiting for WAL
- `WALRCV_RESTARTING`: Restart in progress
- `WALRCV_STOPPING`: Shutdown in progress

**Monitoring Query**:
```sql
-- Check receiver state
SELECT
    pid,
    status,
    receive_start_lsn,
    receive_start_tli,
    received_lsn,
    received_tli,
    last_msg_send_time,
    last_msg_receipt_time,
    latest_end_lsn,
    latest_end_time,
    slot_name,
    sender_host,
    sender_port,
    conninfo
FROM pg_stat_wal_receiver;
```

### 2. Authentication and Authorization Issues

#### Replication User Requirements
```sql
-- Verify replication user setup on primary
SELECT rolname, rolreplication, rolcanlogin
FROM pg_roles
WHERE rolname = 'replicator';

-- Check pg_hba.conf entries
-- Required entry: host replication replicator standby_ip/32 md5
```

#### SSL/TLS Debugging
```sql
-- Check SSL status on active connections
SELECT pid, application_name, client_addr, ssl, ssl_version, ssl_cipher
FROM pg_stat_ssl
JOIN pg_stat_replication USING (pid);
```

## Process Crash Analysis

### 1. WalSender Crash Diagnosis

#### Common Crash Scenarios

**Memory Corruption in Send Buffer**:
- **Function**: `WalSndWriteData()` in `src/backend/replication/walsender.c:3764-3798`
- **Symptom**: Segmentation fault during message construction
- **Diagnosis**: Check for buffer overflow in `pq_putmessage_noblock()`

**Signal Handling Issues**:
- **Function**: `WalSndLoop()` signal handling
- **Symptom**: Unexpected termination during `CHECK_FOR_INTERRUPTS()`
- **Diagnosis**: Examine signal timing and handler state

#### Log Analysis Techniques

**PostgreSQL Log Patterns**:
```bash
# WalSender crash patterns
grep -A 5 -B 5 "walsender.*terminated" postgresql.log

# Segmentation fault analysis
grep -A 10 "segfault.*walsender" /var/log/messages

# Connection termination patterns
grep "connection.*replication" postgresql.log | grep -E "(closed|terminated|lost)"
```

**System-Level Debugging**:
```bash
# Core dump analysis (if enabled)
gdb postgres core.walsender.pid
(gdb) bt
(gdb) info registers
(gdb) x/20i $pc

# Memory mapping analysis
cat /proc/PID/maps | grep -E "(heap|stack)"
```

### 2. WalReceiver Crash Diagnosis

#### Crash Point Analysis

**WAL Write Failures**:
- **Function**: `XLogWalRcvWrite()` in `src/backend/replication/walreceiver.c:860-952`
- **Common Causes**: Disk space exhaustion, I/O errors, filesystem corruption

**Message Processing Errors**:
- **Function**: `XLogWalRcvProcessMsg()` in `src/backend/replication/walreceiver.c:1048-1175`
- **Common Causes**: Protocol violations, corrupt data, timeline mismatches

#### Recovery Strategies

**Automatic Recovery**:
```sql
-- Check for automatic restart attempts
SELECT
    pg_stat_get_wal_receiver_pid() as current_pid,
    conninfo,
    last_msg_receipt_time
FROM pg_stat_wal_receiver;
```

**Manual Recovery**:
```sql
-- Stop and restart WAL receiver
SELECT pg_reload_conf();  -- Reload configuration
-- Or restart PostgreSQL service
```

## Performance Bottleneck Identification

### 1. CPU Bottlenecks

#### WalSender CPU Usage Analysis

**Hot Code Paths** (from `critical_symbols.txt`):
1. `XLogSendPhysical()` - Network transmission overhead
2. `ProcessRepliesIfAny()` - Standby feedback processing
3. `WalSndComputeSleeptime()` - Sleep duration calculation

**Profiling Techniques**:
```bash
# Profile WalSender process
perf record -p $(pgrep -f "walsender") -g
perf report --no-children

# System-wide analysis
perf top -p $(pgrep postgres)

# Function-level analysis
perf record -g ./postgres --walsender
perf annotate XLogSendPhysical
```

#### Startup Process CPU Usage

**Replay Bottlenecks**:
- **Function**: `XLogReadRecord()` - WAL record parsing overhead
- **Function**: `GetCurrentReplayRecPtr()` - State query frequency

**Monitoring Commands**:
```sql
-- Check replay progress rate
SELECT
    pg_last_wal_replay_lsn(),
    pg_last_wal_receive_lsn(),
    extract(epoch from now() - pg_last_xact_replay_timestamp()) as lag_seconds;
```

### 2. Memory Bottlenecks

#### Shared Memory Analysis

**Key Structures** (from implementation analysis):
- `WalSndCtlData`: WalSender control data
- `WalRcvData`: WalReceiver global state
- `XLogCtlData`: WAL control structure

**Memory Usage Monitoring**:
```sql
-- Check shared memory usage
SELECT
    name,
    setting,
    unit,
    context
FROM pg_settings
WHERE name LIKE '%shared%' OR name LIKE '%wal%buffer%';

-- WAL buffer utilization
SELECT
    wal_buffers_full,
    wal_write,
    wal_sync
FROM pg_stat_wal;
```

#### Process Memory Analysis

**WalSender Memory Usage**:
```bash
# Process memory mapping
pmap -x $(pgrep -f "walsender")

# Memory usage over time
top -p $(pgrep -f "walsender") -d 1

# Detailed memory analysis
cat /proc/$(pgrep -f "walsender")/status
```

### 3. I/O Bottlenecks

#### WAL Write Performance

**Primary Side Analysis**:
```sql
-- WAL write timing
SELECT
    wal_write_time,
    wal_sync_time,
    wal_records,
    wal_bytes
FROM pg_stat_wal;
```

**Disk I/O Monitoring**:
```bash
# Monitor WAL directory I/O
iostat -x 1 | grep -E "(Device|pg_wal)"

# Check for I/O wait
vmstat 1 | head -20

# Detailed I/O analysis
iotop -p $(pgrep postgres)
```

#### Network I/O Performance

**Bandwidth Utilization**:
```bash
# Monitor network usage during replication
iftop -i eth0 -f "port 5432"

# Network statistics
cat /proc/net/dev | grep eth0

# TCP buffer analysis
ss -i | grep ":5432"
```

## Network Debugging

### 1. Protocol-Level Debugging

#### Replication Protocol Analysis

**Message Types** (from `network_protocol_messages.mermaid`):
- `'w'` - WAL data message
- `'k'` - Keepalive message
- `'r'` - Standby status reply
- `'h'` - Hot standby feedback

**Protocol Debugging Tools**:
```bash
# Capture replication traffic
tcpdump -i any -A "port 5432 and host standby_ip"

# Analyze packet timing
tcpdump -i any -ttt "port 5432" | grep -E "(walsender|walreceiver)"

# SSL/TLS analysis
openssl s_client -connect primary:5432 -debug
```

#### Message Flow Analysis

**Keepalive Mechanism**:
- **Function**: `WalSndKeepaliveIfNecessary()` sends periodic keepalives
- **Timeout**: Configured by `wal_sender_timeout` parameter
- **Response**: Standby must respond within timeout period

**Debugging Keepalive Issues**:
```sql
-- Check keepalive timing
SELECT
    application_name,
    last_msg_send_time,
    last_msg_receipt_time,
    extract(epoch from (now() - last_msg_receipt_time)) as seconds_since_last_msg
FROM pg_stat_replication;
```

### 2. Network Connectivity Issues

#### Firewall and Routing

**Connection Testing**:
```bash
# Test basic connectivity
telnet primary_host 5432

# Check routing
traceroute primary_host

# MTU discovery
ping -M do -s 1472 primary_host
```

#### Bandwidth and Latency

**Network Performance Testing**:
```bash
# Bandwidth test
iperf3 -c primary_host -p 5432 -t 30

# Latency measurement
ping -c 100 primary_host | tail -1

# Packet loss detection
mtr primary_host --report --report-cycles 100
```

## Shared Memory Debugging

### 1. Condition Variable Analysis

#### Inter-Process Coordination

**Key Condition Variables** (from implementation):
- `wal_flush_cv`: Physical replication wakeup
- `wal_replay_cv`: Logical replication wakeup
- `walRcvStoppedCV`: WalReceiver shutdown coordination

**Debugging Coordination Issues**:
```bash
# Check process states
ps aux | grep -E "(walsender|walreceiver|startup)"

# Monitor condition variable usage
strace -e futex -p $(pgrep -f "walsender")

# Latch usage analysis
strace -e epoll_wait -p $(pgrep postgres)
```

### 2. Atomic Operations Debugging

#### Memory Ordering Issues

**Critical Atomic Operations**:
- `pg_atomic_write_u64()`: LSN updates
- `pg_memory_barrier()`: Memory ordering enforcement
- Spinlock operations for shared state

**Race Condition Detection**:
```bash
# Thread sanitizer (if compiled with support)
export TSAN_OPTIONS="halt_on_error=1:abort_on_error=1"

# Helgrind analysis
valgrind --tool=helgrind --read-var-info=yes postgres --walsender
```

## Timeline Debugging

### 1. Timeline Switch Analysis

#### Timeline Consistency Checking

**Function**: `readTimeLineHistory()` loads timeline history
**Location**: `src/backend/access/transam/timeline.c:85-201`

**Timeline Debugging**:
```sql
-- Check current timeline
SELECT pg_current_wal_lsn(), timeline_id
FROM pg_control_checkpoint();

-- Timeline history analysis
\! ls -la $PGDATA/pg_wal/timeline.history*

-- Check for timeline switches in logs
```

**Log Pattern Analysis**:
```bash
# Timeline switch patterns
grep -E "timeline.*switch" postgresql.log

# Recovery timeline selection
grep -A 5 -B 5 "timeline" postgresql.log | grep recovery
```

### 2. Timeline Mismatch Resolution

#### Synchronization Issues

**Common Scenarios**:
1. Standby ahead of primary timeline
2. Missing timeline history files
3. Timeline branch conflicts during promotion

**Resolution Steps**:
```sql
-- Stop WAL receiver
SELECT pg_reload_conf();

-- Check timeline status
SELECT pg_current_wal_lsn(),
       pg_last_wal_receive_lsn(),
       pg_last_wal_replay_lsn();

-- Manual timeline correction (if necessary)
-- This requires careful analysis and potential standby rebuild
```

## Replication Slot Debugging

### 1. Slot State Analysis

#### Slot Management Functions

**Key Functions**:
- `walrcv_create_slot()`: Slot creation on standby
- `CheckXLogRemoved()`: WAL retention checking
- Slot state tracking in shared memory

**Slot Monitoring**:
```sql
-- Check slot status
SELECT
    slot_name,
    plugin,
    slot_type,
    datoid,
    temporary,
    active,
    active_pid,
    xmin,
    catalog_xmin,
    restart_lsn,
    confirmed_flush_lsn,
    wal_status,
    safe_wal_size
FROM pg_replication_slots;

-- Check WAL retention
SELECT
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as wal_retained_bytes,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as wal_retained
FROM pg_replication_slots;
```

### 2. Slot Cleanup and Maintenance

#### Automatic Cleanup Issues

**WAL Removal Prevention**:
- Slots prevent WAL removal beyond `restart_lsn`
- Inactive slots can cause WAL accumulation
- Monitor `wal_status` for retention warnings

**Cleanup Procedures**:
```sql
-- Check for problematic slots
SELECT slot_name, active, wal_status, safe_wal_size
FROM pg_replication_slots
WHERE wal_status = 'lost' OR NOT active;

-- Manual slot removal (if safe)
SELECT pg_drop_replication_slot('slot_name');

-- Slot recreation
SELECT pg_create_physical_replication_slot('new_slot_name');
```

## Log Analysis and Monitoring

### 1. Structured Log Analysis

#### Key Log Patterns

**WalSender Events**:
```bash
# Connection establishment
grep "replication connection.*authenticated" postgresql.log

# State changes
grep -E "walsender.*(starting|stopping|streaming)" postgresql.log

# Error patterns
grep -E "walsender.*ERROR" postgresql.log
```

**WalReceiver Events**:
```bash
# Receiver startup
grep "walreceiver.*started" postgresql.log

# Connection issues
grep -E "walreceiver.*(connection|timeout)" postgresql.log

# Write/flush operations
grep -E "walreceiver.*(wrote|flushed)" postgresql.log
```

### 2. Performance Monitoring Setup

#### Metrics Collection

**Key Performance Indicators**:
1. Replication lag (bytes and time)
2. WAL generation rate
3. Network throughput
4. Disk I/O for WAL operations
5. Process CPU usage

**Monitoring Query Collection**:
```sql
-- Comprehensive replication status
SELECT
    r.application_name,
    r.client_addr,
    r.state,
    r.sent_lsn,
    r.write_lsn,
    r.flush_lsn,
    r.replay_lsn,
    r.write_lag,
    r.flush_lag,
    r.replay_lag,
    pg_wal_lsn_diff(pg_current_wal_lsn(), r.flush_lsn) as lag_bytes,
    s.wal_records,
    s.wal_bytes,
    s.wal_write_time,
    s.wal_sync_time
FROM pg_stat_replication r
CROSS JOIN pg_stat_wal s;
```

#### Alerting Thresholds

**Recommended Alert Levels**:
- **Lag Warning**: > 100MB or > 10 seconds
- **Lag Critical**: > 1GB or > 60 seconds
- **Connection Loss**: No active WalReceiver for > 30 seconds
- **WAL Accumulation**: > 10GB retained WAL
- **Disk Space**: < 20% free in WAL directory

## Error Recovery Procedures

### 1. Automatic Recovery Mechanisms

#### Built-in Recovery Features

**WalReceiver Restart Logic**:
- Automatic reconnection on connection loss
- Timeline validation and recovery
- Partial WAL record handling

**WalSender Resilience**:
- Network timeout handling
- Client disconnect detection
- Buffer overflow protection

### 2. Manual Recovery Procedures

#### Complete Replication Reset

**When to Use**:
- Severe timeline conflicts
- Corrupt WAL data on standby
- Persistent connection failures

**Procedure**:
```bash
# 1. Stop standby PostgreSQL
systemctl stop postgresql

# 2. Remove WAL and data (DANGEROUS - backup first!)
rm -rf $PGDATA/pg_wal/*
rm -rf $PGDATA/base/*

# 3. Take fresh base backup
pg_basebackup -h primary -D $PGDATA -U replicator -R -W

# 4. Restart standby
systemctl start postgresql
```

#### Incremental Recovery

**Partial WAL Loss Recovery**:
```sql
-- Check for missing WAL
SELECT pg_walfile_name(pg_last_wal_receive_lsn());
\! ls $PGDATA/pg_wal/

-- Recovery from archive (if available)
-- Configure restore_command in postgresql.conf
```

## Performance Tuning Integration

> **Next Steps**: For comprehensive performance optimization strategies based on these debugging findings, see:
> [Streaming Replication Performance Tuning Guide](streaming_replication_performance_tuning.md)

This debugging reference provides the diagnostic foundation necessary for effective performance tuning and system optimization.

## References and Cross-Links

### Implementation Documentation
- [Streaming Replication Implementation Guide](streaming_replication_implementation_guide.md)
- [Implementation Coverage Report](implementation_coverage_report.md)

### Detailed Component Analysis
- [WalSender Processing Details](detailed_walsender_processing.md)
- [WalReceiver Processing Details](detailed_walreceiver_processing.md)
- [Startup Process Integration](detailed_startup_replay.md)
- [BGWriter Interaction Analysis](detailed_bgwriter_interaction.md)

### Architecture Documentation
- [Primary WAL Flow](streaming_replication_detailed/primary_side_processing/wal_generation_to_walsender.md)
- [Standby Processing](streaming_replication_detailed/standby_side_processing/walreceiver_operations.md)
- [Inter-Process Coordination](streaming_replication_detailed/inter_process_coordination/standby_feedback_protocol.md)