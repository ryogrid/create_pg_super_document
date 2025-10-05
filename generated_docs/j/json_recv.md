# json_recv

## Location
[src/backend/utils/adt/json.c:150-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L150-L176)

## Overview
Deserializes binary-formatted JSON data received from PostgreSQL's binary protocol back into the internal JSON text representation with validation.

## Definition

```c
Datum
json_recv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the counterpart to  and handles the deserialization of JSON data that was transmitted using PostgreSQL's binary protocol. It extracts the JSON text from the binary message buffer, validates the JSON syntax, and converts it back to PostgreSQL's internal JSON representation (text format). This function is crucial for maintaining data integrity when receiving JSON data over the binary protocol.

The function performs comprehensive validation of the received JSON data, including proper encoding handling based on the database's character encoding. Unlike , this function uses  which will raise an error if the JSON is invalid, ensuring that corrupted or malformed binary data cannot result in invalid JSON being stored.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0: StringInfo pointer to the binary message buffer containing serialized JSON data

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts pointer argument from function call
  - : Buffer structure containing binary message data
  - : Extracts text data from binary message buffer
  - : Structure for JSON lexical analysis context
  - : Initializes JSON parser with specific string length and encoding
  - : Retrieves current database character encoding
  - : Validates JSON syntax, reporting errors if invalid
  - : Semantic action that performs no operations during parsing
  - : Converts C-string with known length to PostgreSQL text
  - : Returns text datum to PostgreSQL
- Called from (representative examples):
  - PostgreSQL binary protocol handler when receiving JSON data from clients
  - Binary data import functions
  - Replication and restore systems processing binary format

## Notes and Other Information
- Handles character encoding correctly based on database settings
- Performs strict JSON validation that will raise errors for invalid data
- More robust error handling compared to  since binary protocol expects strict validation
- Works with exact byte lengths to handle binary data precisely
- Part of the binary protocol infrastructure ensuring data integrity during transmission
- The binary format is platform-independent and includes all necessary metadata
- Essential for maintaining consistency when using PostgreSQL's binary protocol mode

## Simplified Source

```c
Datum
json_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    char *json_str;
    int nbytes;
    JsonLexContext lex;

    // Extract text data from binary message buffer
    json_str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

    // Validate JSON syntax with proper encoding
    makeJsonLexContextCstringLen(&lex, json_str, nbytes,
                                GetDatabaseEncoding(), false);
    pg_parse_json_or_ereport(&lex, &nullSemAction);

    // Convert to PostgreSQL text format
    PG_RETURN_TEXT_P(cstring_to_text_with_len(json_str, nbytes));
}
```