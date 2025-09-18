# xact_desc_subxacts

## Location
src/backend/access/rmgrdesc/xactdesc.c: 301 - 313

## Overview
xact_desc_subxacts is a static helper function that formats subtransaction IDs into a human-readable string for WAL record descriptions.

## Definition
```c
static void xact_desc_subxacts(StringInfo buf, int nsubxacts, TransactionId *subxacts)
```

## Detailed Description
This utility function is used internally by the xact description functions to append formatted subtransaction information to output buffers. It takes an array of TransactionId values representing subtransactions and formats them as a space-separated list with an appropriate label. The function is essential for making WAL record descriptions comprehensive and human-readable, particularly when analyzing complex transactions with multiple subtransactions. It only outputs information when there are actually subtransactions to describe (nsubxacts > 0).

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted subtransaction information to
- `nsubxacts`: Number of subtransactions in the subxacts array
- `subxacts`: Array of TransactionId values representing the subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString
  - appendStringInfo
  - TransactionId (type)
- Called from (representative examples):
  - [xact_desc_commit](xact_desc_commit.md)
  - [xact_desc_abort](xact_desc_abort.md)
  - [xact_desc_prepare](xact_desc_prepare.md)

## Notes and Other Information
- Static function, only used within xactdesc.c
- Simple formatting function with minimal processing overhead
- Output format: "; subxacts: id1 id2 id3"
- Only outputs when nsubxacts > 0, avoiding empty sections in descriptions
- Uses %u format specifier for TransactionId values
- Critical for debugging nested transactions and understanding transaction hierarchies in WAL analysis
- Complementary to xact_desc_relations for providing complete transaction context