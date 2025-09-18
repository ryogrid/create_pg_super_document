# SeqTableData

## Location
[src/backend/commands/sequence.c:76-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L76-L87)

## Overview
A data structure that stores per-session state for sequences to maintain nextval/currval behavior and caching information across transaction boundaries.

## Definition
```c
typedef struct SeqTableData
{
    Oid             relid;          /* pg_class OID of this sequence (hash key) */
    RelFileNumber   filenumber;     /* last seen relfilenumber of this sequence */
    LocalTransactionId lxid;       /* xact in which we last did a seq op */
    bool            last_valid;     /* do we have a valid "last" value? */
    int64           last;           /* value last returned by nextval */
    int64           cached;         /* last value already cached for nextval */
    /* if last != cached, we have not used up all the cached values */
    int64           increment;      /* copy of sequence's increment field */
    /* note that increment is zero until we first do nextval_internal() */
} SeqTableData;
```

## Detailed Description
The `SeqTableData` struct is a critical component of PostgreSQL's sequence management system that maintains per-session state for sequences. This structure is stored in a hash table to track sequence state across the current database session, which is necessary because the relcache may discard entries and cannot be relied upon for persistent sequence state. Each entry represents a sequence that has been accessed in the current session and stores essential information for implementing nextval/currval semantics and value caching optimization.

The structure enables PostgreSQL to maintain sequence continuity within a session while supporting performance optimizations through value caching. When a sequence operation occurs, PostgreSQL looks up or creates a SeqTableData entry to track the sequence's state and handle caching logic.

## Parameters / Member Variables
- `relid`: The OID from pg_class that uniquely identifies this sequence (serves as the hash key)
- `filenumber`: The last observed RelFileNumber for this sequence, used to detect sequence redefinition
- `lxid`: LocalTransactionId of the transaction that last performed a sequence operation
- `last_valid`: Boolean flag indicating whether the "last" field contains a valid value
- `last`: The most recent value returned by nextval for this sequence
- `cached`: The highest value that has been pre-allocated/cached for future nextval calls
- `increment`: A cached copy of the sequence's increment value (zero until first nextval_internal call)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](../R/RelFileNumber.md) (data type for file numbering)
  - LocalTransactionId (data type for transaction tracking)
- Called from (representative examples):
  - SeqTable (type definition at src/backend/commands/sequence.c:89, 97)
  - [create_seq_hashtable](../c/create_seq_hashtable.md) (src/backend/commands/sequence.c:1118)

## Notes and Other Information
- This structure is stored in a session-level hash table indexed by sequence OID
- The caching mechanism (last vs cached values) optimizes performance by reducing disk I/O for sequence operations
- The increment field remains zero until the first nextval_internal() call to avoid unnecessary sequence metadata reads
- Essential for maintaining ACID properties and session consistency for sequence operations
- Part of PostgreSQL's strategy to work around relcache limitations for sequence state management