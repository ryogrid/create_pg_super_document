# xl_seq_rec

## Location
src/include/commands/sequence.h: 48 - 52

## Overview
xl_seq_rec is a Write-Ahead Logging (WAL) record structure that stores information needed to replay sequence operations during crash recovery.

## Definition


## Detailed Description
This structure is used in PostgreSQL's WAL system to log sequence state changes for crash recovery and replication. When a sequence value is incremented (via nextval()), the operation is logged to WAL using this record format. The structure contains the minimal header information needed to identify which sequence relation was modified, followed by the actual sequence tuple data that gets appended after the structure.

The xl_seq_rec is specifically used with the XLOG_SEQ_LOG WAL record type (defined as 0x00) and is part of the RM_SEQ_ID resource manager for sequence operations. During recovery, the seq_redo() function uses this structure to replay sequence modifications and restore the correct sequence state.

## Parameters / Member Variables
- : RelFileLocator that uniquely identifies the sequence relation (contains space OID, database OID, and relation number)

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator (embedded structure for relation identification)
- Called from (representative examples):
  - nextval_internal (creates xl_seq_rec for WAL logging during sequence value generation)
  - do_setval (creates xl_seq_rec when explicitly setting sequence values)
  - seq_redo (reads xl_seq_rec during WAL replay to restore sequence state)
  - seq_desc (reads xl_seq_rec to format human-readable WAL record descriptions)
  - fill_seq_fork_with_data (uses xl_seq_rec for sequence relation initialization)

## Notes and Other Information
- The comment "SEQUENCE TUPLE DATA FOLLOWS AT THE END" indicates that the actual FormData_pg_sequence_data follows this header in the WAL record
- The structure uses a variable-length design where the sequence tuple data is appended after the fixed-size xl_seq_rec header
- During WAL logging, XLogRegisterData() is called twice: once for the xl_seq_rec header and once for the sequence tuple data
- The seq_desc() function extracts the RelFileLocator components to display sequence relation information in WAL record descriptions
- This structure is part of PostgreSQL's crash recovery mechanism and is essential for maintaining sequence consistency across system failures
- The REGBUF_WILL_INIT flag is used when registering the buffer, indicating that the entire buffer will be reinitialized during replay