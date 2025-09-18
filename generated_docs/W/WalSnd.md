# WalSnd

## Location
src/include/replication/walsender_private.h: 42 - 86

## Overview
WalSnd is a shared memory structure that represents the state and control information for each WAL sender process in PostgreSQL's replication system. Each walsender process has its own WalSnd struct in shared memory to track replication progress and coordinate with standby servers.

## Definition


## Detailed Description
The WalSnd structure serves as the central coordination point for each WAL sender process in PostgreSQL's streaming replication architecture. It maintains critical state information about the replication connection, tracks replication progress, and provides synchronization mechanisms between the walsender process and other backend processes.

This structure is stored in shared memory and protected by a spinlock mutex to ensure thread-safe access. The structure tracks both the sender-side state (what has been sent) and receiver-side acknowledgments (what has been written, flushed, and applied on the standby). This bidirectional tracking enables features like synchronous replication and lag monitoring.

The struct is designed to support both physical and logical replication through the ReplicationKind field, and integrates with PostgreSQL's synchronous replication infrastructure through the sync_standby_priority field.

## Parameters / Member Variables
- : Process ID of the walsender process; 0 indicates an inactive slot
- : Current state of the walsender (WalSndState enum values like WALSNDSTATE_STARTUP, WALSNDSTATE_STREAMING, etc.)
- : WAL location up to which data has been sent to the standby
- : Flag indicating whether the currently open WAL file needs to be reloaded (used for WAL file rotation)
- : WAL location confirmed as written by the standby server
- : WAL location confirmed as flushed to disk by the standby server
- : WAL location confirmed as applied/replayed by the standby server
- : Measured time lag for write acknowledgments (-1 if unknown)
- : Measured time lag for flush acknowledgments (-1 if unknown)
- : Measured time lag for apply acknowledgments (-1 if unknown)
- : Priority in synchronous_standby_names list (0 if not listed)
- : Spinlock protecting shared variables in this structure
- : Pointer to walsender's latch for inter-process communication (NULL if inactive)
- : Timestamp of the last message received from the standby
- : Type of replication (physical or logical)

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - WalSndState
  - XLogRecPtr
  - TimeOffset
  - slock_t
  - Latch
  - TimestampTz
  - ReplicationKind

- Called from (representative examples):
  - SyncRepGetCandidateStandbys
  - ProcessStandbyReplyMessage
  - ProcessStandbyHSFeedbackMessage
  - InitWalSenderSlot
  - WalSndKill
  - XLogSendPhysical
  - XLogSendLogical
  - WalSndSetState
  - WalSndShmemInit

## Notes and Other Information
- The structure is protected by a spinlock mutex, but some members are only written by the walsender process itself and can be read without holding the spinlock
- The  and  fields always require the spinlock for all accesses
- WAL locations (write, flush, apply) may be invalid if the standby has not yet offered values
- Lag measurements are critical for monitoring replication performance and detecting replication delays
- The structure supports PostgreSQL's synchronous replication by tracking standby priorities and acknowledgment states
- Memory for WalSnd structures is allocated in shared memory during PostgreSQL startup
- The structure is defined in src/include/replication/walsender_private.h:42-86