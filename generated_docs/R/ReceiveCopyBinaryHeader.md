# ReceiveCopyBinaryHeader

## Location
[src/backend/commands/copyfromparse.c:190-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L190-L244)

## Overview
Validates and processes the binary file header for COPY FROM operations in binary format, ensuring the input conforms to PostgreSQL's binary COPY format specification.

## Definition
void ReceiveCopyBinaryHeader(CopyFromState cstate)

## Detailed Description
ReceiveCopyBinaryHeader reads and validates the header of a binary COPY file format. It performs several validation checks: verifies the 11-byte binary signature matches the expected PostgreSQL binary format signature, reads and validates the flags field (rejecting WITH OIDS format and unrecognized critical flags), reads the header extension length, and skips any extension header data. The function ensures that the binary COPY data conforms to the expected format before proceeding with data processing.

## Parameters / Member Variables
- `cstate`: CopyFromState structure containing the current state and configuration of the COPY FROM operation, used to read binary data from the input source

## Dependencies
- Functions called/Symbols referenced:
  - [CopyReadBinaryData](../C/CopyReadBinaryData.md) (read raw binary data from input)
  - [CopyGetInt32](../C/CopyGetInt32.md) (read 32-bit integer values)
  - memcmp (compare binary signature)
  - ereport/errcode/errmsg (error reporting)
  - BinarySignature (constant defining expected signature)
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (src/backend/commands/copyfrom.c:1766)

## Notes and Other Information
- The function strictly validates the binary format header and reports errors for any deviations
- Rejects files with WITH OIDS format (legacy feature no longer supported)
- Handles extension headers by skipping them, allowing for future format extensibility
- Critical flags in the upper 16 bits of the flags field are rejected to ensure forward compatibility
- The 11-byte signature must exactly match PostgreSQL's binary COPY format identifier

## Simplified Source
```c
void ReceiveCopyBinaryHeader(CopyFromState cstate) {
    char readSig[11];
    int32 tmp;

    // Validate 11-byte binary signature
    if (CopyReadBinaryData(cstate, readSig, 11) != 11 ||
        memcmp(readSig, BinarySignature, 11) != 0) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                       errmsg("COPY file signature not recognized")));
    }

    // Read and validate flags field
    if (!CopyGetInt32(cstate, &tmp)) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                       errmsg("invalid COPY file header (missing flags)")));
    }

    // Reject WITH OIDS format and unknown critical flags
    if ((tmp & (1 << 16)) != 0) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                       errmsg("invalid COPY file header (WITH OIDS)")));
    }
    if ((tmp >> 16) != 0) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                       errmsg("unrecognized critical flags in COPY file header")));
    }

    // Read header extension length and skip extension data
    if (!CopyGetInt32(cstate, &tmp) || tmp < 0) {
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                       errmsg("invalid COPY file header (missing length)")));
    }

    // Skip extension header if present
    while (tmp-- > 0) {
        if (CopyReadBinaryData(cstate, readSig, 1) != 1) {
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                           errmsg("invalid COPY file header (wrong length)")));
        }
    }
}
```