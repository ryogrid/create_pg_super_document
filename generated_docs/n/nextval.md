# nextval

## Location
[src/backend/commands/sequence.c:593-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L593-L614)

## Overview
Provides the PostgreSQL SQL function interface for obtaining the next value from a sequence, accepting a sequence name as a text argument.

## Definition
```c
Datum nextval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL-callable nextval() function that takes a sequence name as a text parameter and returns the next value from that sequence. It serves as a wrapper around the core nextval_internal() function, handling the conversion from text sequence name to sequence OID.

The function parses the input text to extract a qualified sequence name, resolves it to a relation OID, and then delegates to nextval_internal() for the actual sequence value generation. While this function is no longer exported as a pg_proc entry in recent PostgreSQL versions, it is maintained for backward compatibility with C code that may call it directly.

A notable design trade-off exists in this function: it uses NoLock when resolving the sequence name for performance reasons, relying on nextval_internal's caching mechanism to handle locking efficiently, though this approach is not completely safe in the presence of concurrent DDL operations.

## Parameters / Member Variables
- Function argument accessed via PG_GETARG_TEXT_PP(0): The sequence name as text input

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (to get text argument)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [textToQualifiedNameList](../t/textToQualifiedNameList.md)  
  - RangeVarGetRelid (with NoLock)
  - [nextval_internal](nextval_internal.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - [ttdummy](../t/ttdummy.md) (in regress.c for testing)

## Notes and Other Information
- No longer exported as a pg_proc entry but maintained for C API compatibility
- Uses NoLock strategy for performance, trading some safety for efficiency in concurrent DDL scenarios
- The performance vs. safety trade-off is documented as potentially needing future revision
- Returns INT64 (bigint) values to support the full range of sequence values
- Part of the legacy text-based sequence interface alongside the newer OID-based approach

## Simplified Source

```c
Datum nextval(PG_FUNCTION_ARGS) {
    // Get sequence name from text argument
    text *seqin = PG_GETARG_TEXT_PP(0);

    // Convert text name to RangeVar and resolve to OID
    RangeVar *sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin));
    Oid relid = RangeVarGetRelid(sequence, NoLock, false);

    // Get next value from sequence
    PG_RETURN_INT64(nextval_internal(relid, true));
}
```