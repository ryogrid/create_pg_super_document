# PGresult

## Location
[src/interfaces/libpq/libpq-fe.h:198-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L198-L203)

## Overview
PGresult encapsulates the result of a query execution, representing a single SQL command's result set including data rows, column metadata, status information, and any error details.

## Definition


The actual structure definition is in  as .

## Detailed Description
PGresult is the fundamental result container in libpq that holds all information returned from executing SQL commands. Each PGresult represents the result of a single SQL command - if a query string contains multiple commands, multiple PGresult objects will be returned.

The structure contains:
- **Result Data**: Actual query results including tuples (rows) and their values
- **Metadata**: Column descriptions, data types, and formatting information  
- **Status Information**: Execution status, command tag, and row counts
- **Error Handling**: Error messages and detailed error field information
- **Memory Management**: Efficient block-based memory allocation for large result sets

Key design features:
- Opaque structure - applications access data through libpq API functions
- Efficient memory management with block allocation strategy
- Support for both text and binary result formats
- Comprehensive error reporting with structured error fields
- Event system integration for custom processing

## Parameters / Member Variables
- **ntups**:  - Number of tuples (rows) in the result set
- **numAttributes**:  - Number of columns in each tuple  
- **attDescs**:  - Array of column attribute descriptors
- **tuples**:  - 2D array containing all tuple data values
- **tupArrSize**:  - Allocated size of the tuples array for memory management
- **numParameters**:  - Number of parameters for prepared statements
- **paramDescs**:  - Parameter descriptors for prepared statements
- **resultStatus**:  - Overall execution status (success, error, etc.)
- **cmdStatus**:  - Command status string from server
- **binary**:  - Flag indicating binary (1) vs text (0) tuple format
- **noticeHooks**:  - Notice message processing callbacks
- **events**:  - Registered event processors
- **client_encoding**:  - Character encoding for text data
- **errMsg**:  - Primary error message if result indicates error
- **errFields**:  - Structured error information fields
- **errQuery**:  - SQL query text that triggered the error
- **null_field**:  - Shared null string for NULL attribute values
- **curBlock**:  - Current memory allocation block
- **memorySize**:  - Total memory allocated for this result

## Dependencies
- Functions called/Symbols referenced:
  - pg_result (the underlying struct type)
  - PGresAttDesc (column attribute descriptors)
  - PGresAttValue (individual field values)
  - ExecStatusType (result status enumeration)
- Called from (representative examples):
  - PQexec, PQexecParams - Execute queries and return results
  - PQgetResult - Retrieve results from asynchronous queries
  - PQclear - Free PGresult memory
  - PQntuples, PQnfields - Access result dimensions
  - PQgetvalue - Extract individual field values

## Notes and Other Information
- Each PGresult represents exactly one SQL command's result
- Applications must call PQclear() to free PGresult memory when done
- Supports both text and binary result formats
- NULL values are represented by pointers to a shared null_field string
- Memory is allocated in blocks for efficiency with large result sets
- Error results contain detailed structured error information
- Can be used with both synchronous and asynchronous query execution
- Thread safety: PGresult objects are read-only after creation and can be safely shared between threads