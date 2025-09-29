# GUC_check_errcode

## Location
[src/backend/utils/misc/guc.c:6799-6811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6799-L6811)

## Overview
Sets a custom SQL error code for GUC check hook failures, allowing hooks to override the default ERRCODE_INVALID_PARAMETER_VALUE.

## Definition

```c
void
GUC_check_errcode(int sqlerrcode)
```
## Detailed Description
GUC_check_errcode is a utility function designed for use within GUC check hooks that need to report specific SQL error codes when parameter validation fails. By default, failed GUC validations result in ERRCODE_INVALID_PARAMETER_VALUE, but this function allows check hooks to specify more appropriate error codes for their specific validation failures.

The function works in conjunction with other GUC error reporting macros (GUC_check_errmsg, GUC_check_errhint, etc.) to provide comprehensive error information. When a check hook calls this function, the specified error code will be used when the validation failure is reported to the user, providing more contextually relevant error information.

## Parameters / Member Variables
- : The SQL error code to use for the validation failure; typically one of the ERRCODE_* constants

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errcode_value (global variable)
- Called from (representative examples):
  - [check_transaction_read_only](../c/check_transaction_read_only.md)
  - [check_transaction_isolation](../c/check_transaction_isolation.md)  
  - [check_transaction_deferrable](../c/check_transaction_deferrable.md)
  - [check_client_encoding](../c/check_client_encoding.md)
  - [check_session_authorization](../c/check_session_authorization.md)
  - [check_role](../c/check_role.md)
  - [check_default_with_oids](../c/check_default_with_oids.md)
  - [check_synchronous_standby_names](../c/check_synchronous_standby_names.md)

## Notes and Other Information
- Simple assignment function that stores the error code in GUC_check_errcode_value global variable
- Used exclusively within GUC parameter validation check hooks
- Part of PostgreSQL's error reporting mechanism for configuration parameter validation
- Works alongside other GUC_check_* macros that are implemented as direct variable assignments
- The stored error code is retrieved and used when the actual error is reported during GUC processing
- Commonly used in transaction state, encoding, authorization, and replication parameter validation

## Simplified Source

```c
void
GUC_check_errcode(int sqlerrcode)
{
    // Store the custom error code for later use when reporting validation failures
    GUC_check_errcode_value = sqlerrcode;
}
```