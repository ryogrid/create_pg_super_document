# logicalrep_read_stream_abort

## Location
src/backend/replication/logical/proto.c: 1192 - 1216

## Overview
Reads and parses a stream abort message from the logical replication input stream to extract transaction abort information.

## Definition
void logicalrep_read_stream_abort(StringInfo in, LogicalRepStreamAbortData *abort_data, bool read_abort_info)

## Detailed Description
This function is the counterpart to logicalrep_write_stream_abort and is used by logical replication subscribers to parse LOGICAL_REP_MSG_STREAM_ABORT messages. It extracts transaction and subtransaction IDs from the incoming replication stream and populates the provided LogicalRepStreamAbortData structure.

The function supports conditional reading of abort LSN and timestamp information based on the read_abort_info parameter, which must match the corresponding write_abort_info parameter used when the message was written. When abort information is not read, the fields are set to default invalid values.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming stream abort message to parse
- `abort_data`: LogicalRepStreamAbortData structure to be populated with abort information
- `read_abort_info`: Boolean flag controlling whether to read abort_lsn and abort_time from the message

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint
  - pq_getmsgint64
  - Assert (for validation)
  - LogicalRepStreamAbortData (data structure)
  - InvalidXLogRecPtr (constant for invalid LSN)
- Called from (representative examples):
  - apply_handle_stream_abort

## Notes and Other Information
- Includes assertion to ensure abort_data parameter is not NULL
- Conditionally reads abort_lsn and abort_time based on read_abort_info parameter
- Sets abort_lsn to InvalidXLogRecPtr and abort_time to 0 when not reading abort info
- Part of the logical replication subscriber-side message parsing for transaction aborts
- Used to reconstruct transaction abort information on the subscriber side
- Corresponds to the data written by logicalrep_write_stream_abort on the publisher side
- Handles both top-level transaction aborts and subtransaction aborts
- Located in src/backend/replication/logical/proto.c:1192-1216