# VXIDGetDatum

## Location
src/backend/utils/adt/lockfuncs.c: 74 - 92

## Overview
VXIDGetDatum constructs a text representation of a Virtual Transaction ID (VXID) for use in PostgreSQL's lock status reporting system.

## Definition
static Datum VXIDGetDatum(ProcNumber procNumber, LocalTransactionId lxid)

## Detailed Description
VXIDGetDatum is a static utility function that formats a Virtual Transaction ID into a human-readable text representation. The function creates a string in the format "<procNumber>/<lxid>" where procNumber is displayed as a signed decimal and lxid as an unsigned decimal. This representation is specifically used by PostgreSQL's lock status functions to provide readable transaction identifiers. The function uses snprintf for safe string formatting and converts the resulting C string to a PostgreSQL text datum using CStringGetTextDatum.

## Parameters / Member Variables
- `procNumber`: The process number component of the VXID, representing which backend process owns this transaction
- `lxid`: The local transaction ID component, a unique identifier for the transaction within that process

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (C standard library function)
  - CStringGetTextDatum (PostgreSQL datum conversion function)
- Referenced types:
  - ProcNumber
  - LocalTransactionId
- Called from (representative examples):
  - [pg_lock_status](../p/pg_lock_status.md) (multiple locations: lines 305, 355, 421)

## Notes and Other Information
- This function is currently only used within pg_lock_status functionality and is therefore defined as static within lockfuncs.c
- The VXID format matches the representation used by elog.c for consistency across PostgreSQL's logging system
- The function allocates a 32-character buffer which is sufficient for the decimal representation of both components
- Virtual Transaction IDs are used to identify transactions before they are assigned real transaction IDs, providing early tracking capabilities for lock management