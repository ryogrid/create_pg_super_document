# SPI_result_code_string

## Location
[src/backend/executor/spi.c:1972-2056](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1972-L2056)

## Overview
Converts any SPI return code to a human-readable string representation, useful for error messages and debugging.

## Definition
```c
const char *SPI_result_code_string(int code)
```

## Detailed Description
SPI_result_code_string is a utility function that converts SPI return codes (both success and error codes) into their corresponding string representations. The function uses a large switch statement to handle all known SPI return codes, providing descriptive string names that match the symbolic constants. This is particularly useful for error reporting, logging, and debugging purposes where numeric codes need to be presented in a human-readable format.

For unrecognized codes, the function falls back to generating a formatted string indicating the unknown code value. The function uses a static buffer for the fallback case, making it suitable for immediate use but requiring care in multi-threaded contexts or when the return value needs to be stored long-term.

## Parameters / Member Variables
- `code`: An integer representing the SPI return code to be converted to a string. Can be any SPI success code (positive) or error code (negative).

## Dependencies
- Functions called/Symbols referenced:
  - Various SPI return code constants (SPI_OK_*, SPI_ERROR_*)
  - sprintf (for unrecognized codes)
- Called from (representative examples):
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
  - [ri_PlanCheck](../r/ri_PlanCheck.md)
  - [ri_PerformCheck](../r/ri_PerformCheck.md)
  - [plperl_spi_execute_fetch_result](../p/plperl_spi_execute_fetch_result.md)
  - [PLy_cursor_query](../P/PLy_cursor_query.md)
  - [pltcl_process_SPI_result](../p/pltcl_process_SPI_result.md)

## Notes and Other Information
- Returns string literals for recognized codes, ensuring the returned pointer remains valid
- Uses a static buffer for unrecognized codes, which may be overwritten on subsequent calls
- Handles both positive (success) and negative (error) return codes
- Widely used across procedural language implementations (PL/Perl, PL/Python, PL/Tcl)
- Essential for debugging and error reporting in SPI-based code
- The function recognizes all standard SPI return codes including newer ones like MERGE operations

## Simplified Source

```c
const char *SPI_result_code_string(int code) {
    static char buf[64];

    // Map SPI error codes to strings
    switch (code) {
        // Error codes (negative)
        case SPI_ERROR_CONNECT:      return "SPI_ERROR_CONNECT";
        case SPI_ERROR_COPY:         return "SPI_ERROR_COPY";
        case SPI_ERROR_OPUNKNOWN:    return "SPI_ERROR_OPUNKNOWN";
        case SPI_ERROR_UNCONNECTED:  return "SPI_ERROR_UNCONNECTED";
        case SPI_ERROR_ARGUMENT:     return "SPI_ERROR_ARGUMENT";
        case SPI_ERROR_PARAM:        return "SPI_ERROR_PARAM";
        case SPI_ERROR_TRANSACTION:  return "SPI_ERROR_TRANSACTION";
        case SPI_ERROR_NOATTRIBUTE:  return "SPI_ERROR_NOATTRIBUTE";
        case SPI_ERROR_NOOUTFUNC:    return "SPI_ERROR_NOOUTFUNC";
        case SPI_ERROR_TYPUNKNOWN:   return "SPI_ERROR_TYPUNKNOWN";
        case SPI_ERROR_REL_DUPLICATE: return "SPI_ERROR_REL_DUPLICATE";
        case SPI_ERROR_REL_NOT_FOUND: return "SPI_ERROR_REL_NOT_FOUND";

        // Success codes (positive)
        case SPI_OK_CONNECT:         return "SPI_OK_CONNECT";
        case SPI_OK_FINISH:          return "SPI_OK_FINISH";
        case SPI_OK_FETCH:           return "SPI_OK_FETCH";
        case SPI_OK_UTILITY:         return "SPI_OK_UTILITY";
        case SPI_OK_SELECT:          return "SPI_OK_SELECT";
        case SPI_OK_SELINTO:         return "SPI_OK_SELINTO";
        case SPI_OK_INSERT:          return "SPI_OK_INSERT";
        case SPI_OK_DELETE:          return "SPI_OK_DELETE";
        case SPI_OK_UPDATE:          return "SPI_OK_UPDATE";
        case SPI_OK_CURSOR:          return "SPI_OK_CURSOR";
        case SPI_OK_INSERT_RETURNING: return "SPI_OK_INSERT_RETURNING";
        case SPI_OK_DELETE_RETURNING: return "SPI_OK_DELETE_RETURNING";
        case SPI_OK_UPDATE_RETURNING: return "SPI_OK_UPDATE_RETURNING";
        case SPI_OK_REWRITTEN:       return "SPI_OK_REWRITTEN";
        case SPI_OK_REL_REGISTER:    return "SPI_OK_REL_REGISTER";
        case SPI_OK_REL_UNREGISTER:  return "SPI_OK_REL_UNREGISTER";
        case SPI_OK_TD_REGISTER:     return "SPI_OK_TD_REGISTER";
        case SPI_OK_MERGE:           return "SPI_OK_MERGE";
        case SPI_OK_MERGE_RETURNING: return "SPI_OK_MERGE_RETURNING";
    }

    // Handle unrecognized codes
    sprintf(buf, "Unrecognized SPI code %d", code);
    return buf;
}
```