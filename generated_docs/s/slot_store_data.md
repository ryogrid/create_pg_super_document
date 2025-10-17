# slot_store_data

## Location
[src/backend/replication/logical/worker.c:799-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L799-L899)

## Overview
Converts and stores tuple data from logical replication format into a TupleTableSlot, handling both text and binary data formats with appropriate type conversion and error handling.

## Definition
```c
static void
slot_store_data(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
                LogicalRepTupleData *tupleData)
```

## Detailed Description
This function is responsible for populating a TupleTableSlot with data received through logical replication. It handles the conversion of logical replication data formats (both text and binary) into PostgreSQL's internal datum representation. The function performs several key operations:

1. Clears the existing tuple slot to prepare for new data
2. Iterates through all attributes in the local relation descriptor
3. Maps local attributes to remote attributes using the attribute mapping
4. Processes each column based on its data format (text, binary, or NULL)
5. Converts text data using type input functions (OidInputFunctionCall)
6. Converts binary data using type receive functions (OidReceiveFunctionCall)
7. Handles NULL values and missing columns appropriately
8. Stores the completed tuple as a virtual tuple in the slot

The function includes comprehensive error handling and supports schema differences between publisher and subscriber through attribute mapping. It also sets up error callback context for better error reporting during data conversion.

## Parameters / Member Variables
- `slot`: TupleTableSlot where the converted data will be stored
- `rel`: LogicalRepRelMapEntry containing the mapping between local and remote relation attributes
- `tupleData`: LogicalRepTupleData containing the raw replication data with column values and status information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - TupleDescAttr
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md)
  - [OidReceiveFunctionCall](../O/OidReceiveFunctionCall.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - ereport (for error handling)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- Supports both LOGICALREP_COLUMN_TEXT and LOGICALREP_COLUMN_BINARY data formats
- Handles LOGICALREP_COLUMN_UNCHANGED by treating it as NULL (though not expected in this context)
- Dropped attributes and missing values are set to NULL and are expected to be filled later by slot_fill_defaults()
- Includes cursor management for binary data to support re-parsing of the same tuple data
- Validates that binary data is completely consumed during conversion, reporting errors for incomplete consumption
- Uses apply_error_callback_arg to provide context for error reporting during type conversion
- The function assumes that the number of attributes in the slot matches the attribute map length
- Sets both tts_values and tts_isnull arrays for proper NULL handling
- Calls ExecStoreVirtualTuple() to finalize the slot after all data is populated

## Simplified Source

```c
static void slot_store_data(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
                           LogicalRepTupleData *tupleData) {
    int natts = slot->tts_tupleDescriptor->natts;
    int i;

    ExecClearTuple(slot);

    // Process each attribute in the local relation
    for (i = 0; i < natts; i++) {
        Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
        int remoteattnum = rel->attrmap->attnums[i];

        if (!att->attisdropped && remoteattnum >= 0) {
            StringInfo colvalue = &tupleData->colvalues[remoteattnum];

            // Set error context for better error reporting
            apply_error_callback_arg.remote_attnum = remoteattnum;

            if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_TEXT) {
                // Convert text format data
                Oid typinput, typioparam;
                getTypeInputInfo(att->atttypid, &typinput, &typioparam);
                slot->tts_values[i] = OidInputFunctionCall(typinput, colvalue->data,
                                                          typioparam, att->atttypmod);
                slot->tts_isnull[i] = false;
            }
            else if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_BINARY) {
                // Convert binary format data
                Oid typreceive, typioparam;
                colvalue->cursor = 0;  // Reset for re-parsing
                getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
                slot->tts_values[i] = OidReceiveFunctionCall(typreceive, colvalue,
                                                            typioparam, att->atttypmod);

                // Validate complete consumption of binary data
                if (colvalue->cursor != colvalue->len)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                                   errmsg("incorrect binary data format in column %d",
                                          remoteattnum + 1)));
                slot->tts_isnull[i] = false;
            }
            else {
                // NULL or unchanged value
                slot->tts_values[i] = (Datum) 0;
                slot->tts_isnull[i] = true;
            }

            apply_error_callback_arg.remote_attnum = -1;
        }
        else {
            // Dropped or unmapped columns set to NULL
            slot->tts_values[i] = (Datum) 0;
            slot->tts_isnull[i] = true;
        }
    }

    ExecStoreVirtualTuple(slot);
}
```