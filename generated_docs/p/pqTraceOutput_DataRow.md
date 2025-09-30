# pqTraceOutput_DataRow

## Location
[src/interfaces/libpq/fe-trace.c:273-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L273-L290)

## Overview
Outputs a formatted trace of a PostgreSQL DataRow message to a file stream, parsing and displaying the number of fields and their corresponding values.

## Definition
```c
static void pqTraceOutput_DataRow(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing functionality and handles the parsing and output formatting of DataRow protocol messages. The DataRow message is sent by the PostgreSQL server to transmit actual row data as part of query results. Each DataRow message represents one row of query results and contains:

1. Outputs the "DataRow" message type identifier
2. Extracts and displays the number of fields (columns) in the row
3. For each field, extracts and displays:
   - The field length (32-bit integer)
   - The actual field data (if length is not -1)

Fields with length -1 represent NULL values and are skipped in the output. This function processes each field sequentially, providing a complete trace of the row data being transmitted from server to client.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: Pointer to the raw protocol message buffer containing the DataRow message data
- `cursor`: Pointer to an integer tracking the current parsing position within the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputInt16](pqTraceOutputInt16.md) (for the field count)
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (for field lengths)
  - [pqTraceOutputNchar](pqTraceOutputNchar.md) (for field data)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message tracing dispatcher)

## Notes and Other Information
- This is a static function within fe-trace.c, making it internal to the libpq tracing implementation
- NULL values are represented by a field length of -1 and have no associated data bytes
- The function processes variable-length fields, making it one of the more complex message parsers
- DataRow messages can vary significantly in size depending on the data being transmitted
- Part of PostgreSQL's debugging and development tools for analyzing client-server protocol communication
- The function assumes the message buffer contains a valid DataRow message and does not perform extensive error checking
- Field data is output as raw bytes, so binary data may not display readably in trace output

## Simplified Source

```c
static void pqTraceOutput_DataRow(FILE *f, const char *message, int *cursor)
{
    // Output message type identifier
    fprintf(f, "DataRow\t");

    // Extract number of fields in this row
    int nfields = pqTraceOutputInt16(f, message, cursor);

    // Process each field in the row
    for (int i = 0; i < nfields; i++) {
        // Get field length (-1 means NULL)
        int len = pqTraceOutputInt32(f, message, cursor, false);

        // Skip NULL fields, output data for non-NULL fields
        if (len != -1) {
            pqTraceOutputNchar(f, len, message, cursor);
        }
    }
}
```