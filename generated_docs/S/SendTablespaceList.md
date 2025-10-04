# SendTablespaceList

## Location
[src/backend/backup/basebackup_copy.c:378-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L378-L422)

## Overview
SendTablespaceList is a static function that sends a result set describing the tablespace list via the PostgreSQL libpq protocol during base backup operations.

## Definition
```c
static void SendTablespaceList(List *tablespaces)
```

## Detailed Description
This function creates and sends a three-column result set containing information about tablespaces involved in a base backup operation. It iterates through a list of tablespace information structures and sends each tablespace's OID, location path, and size. The function handles NULL values appropriately when tablespace paths are not available or sizes are unknown. Sizes are converted from bytes to kilobytes before transmission.

## Parameters / Member Variables
- `tablespaces`: List * - A linked list containing tablespaceinfo structures with details about each tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - DestRemoteSimple
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitBuiltinEntry](../T/TupleDescInitBuiltinEntry.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - [tablespaceinfo](../t/tablespaceinfo.md) (struct type)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - CStringGetTextDatum
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - [do_tup_output](../d/do_tup_output.md)
  - [end_tup_output](../e/end_tup_output.md)
- Called from (representative examples):
  - [bbsink_copystream_begin_backup](../b/bbsink_copystream_begin_backup.md)

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- Creates a 3-column result set with 'spcoid' (OID), 'spclocation' (TEXT), and 'size' (INT8)
- Handles NULL values for tablespaces with missing path information
- Converts tablespace sizes from bytes to kilobytes before sending
- Size values of -1 are treated as NULL (unknown size)
- Uses the standard PostgreSQL tuple output mechanism for sending structured data
- Essential for communicating tablespace layout information during base backup initialization

## Simplified Source

```c
static void SendTablespaceList(List *tablespaces) {
    DestReceiver *dest;
    TupOutputState *tstate;
    TupleDesc tupdesc;
    ListCell *lc;

    // Create destination for result output
    dest = CreateDestReceiver(DestRemoteSimple);

    // Create tuple descriptor with spcoid, spclocation, size columns
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitBuiltinEntry(tupdesc, 1, "spcoid", OIDOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, "spclocation", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3, "size", INT8OID, -1, 0);

    // Begin tuple output
    tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

    // Send each tablespace as a data row
    foreach(lc, tablespaces) {
        tablespaceinfo *ti = lfirst(lc);
        Datum values[3];
        bool nulls[3] = {0};

        // Handle missing path (NULL case)
        if (ti->path == NULL) {
            nulls[0] = true;
            nulls[1] = true;
        } else {
            values[0] = ObjectIdGetDatum(ti->oid);
            values[1] = CStringGetTextDatum(ti->path);
        }

        // Convert size from bytes to KB, handle unknown size
        if (ti->size >= 0)
            values[2] = Int64GetDatum(ti->size / 1024);
        else
            nulls[2] = true;

        do_tup_output(tstate, values, nulls);
    }

    end_tup_output(tstate);
}
```