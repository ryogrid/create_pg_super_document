# pg_do_encoding_conversion_buf

## Location
[src/backend/utils/mb/mbutils.c:469-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L469-L500)

## Overview
Performs encoding conversion using a caller-provided destination buffer, providing a more memory-efficient alternative to pg_do_encoding_conversion for situations where the output buffer size is controlled.

## Definition
```c
int pg_do_encoding_conversion_buf(Oid proc, int src_encoding, int dest_encoding, unsigned char *src, int srclen, unsigned char *dest, int destlen, bool noError)
```

## Detailed Description
This function provides a buffer-based interface for encoding conversion, where the caller supplies both the source and destination buffers. Unlike pg_do_encoding_conversion which allocates memory for the result, this function writes directly to a pre-allocated destination buffer. The function intelligently limits the input size to ensure the conversion result fits in the destination buffer, accounting for worst-case expansion during encoding conversion.

Key characteristics:
- Uses a caller-provided destination buffer instead of allocating memory
- Limits input size based on destination buffer capacity and MAX_CONVERSION_GROWTH
- Returns the number of bytes written to the destination buffer
- Requires the caller to have already looked up the conversion function procedure
- Provides noError flag for error handling control

## Parameters / Member Variables
- `proc`: OID of the conversion function procedure to use
- `src_encoding`: Source encoding identifier
- `dest_encoding`: Destination encoding identifier  
- `src`: Source buffer containing data to convert
- `srclen`: Length of source data in bytes
- `dest`: Destination buffer for converted output
- `destlen`: Size of destination buffer in bytes
- `noError`: Flag controlling error handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall6 (invokes the conversion procedure)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion utilities)
  - [DatumGetInt32](../D/DatumGetInt32.md) (result extraction)
  - MAX_CONVERSION_GROWTH (expansion factor constant)
- Called from (representative examples):
  - [CopyConvertBuf](../C/CopyConvertBuf.md) (COPY command encoding conversion)
  - [CopyConversionError](../C/CopyConversionError.md) (COPY error handling)
  - [test_enc_conversion](../t/test_enc_conversion.md) (regression testing)

## Notes and Other Information
- More memory-efficient than pg_do_encoding_conversion for controlled buffer scenarios
- Automatically limits input size to prevent destination buffer overflow
- The conversion function interface has known limitations regarding buffer size communication
- Returns actual bytes written, avoiding need for strlen() on result
- Used primarily in performance-critical paths like COPY operations
- Caller must ensure destination buffer is null-terminated appropriately