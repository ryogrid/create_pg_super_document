# pg_saslprep_rc

## Location
src/include/common/saslprep.h: 26 - 30

## Overview
 is an enumeration type that defines return codes for the  function, which implements SASLprep password normalization for SCRAM authentication in PostgreSQL.

## Definition

```c
typedef enum
{
	SASLPREP_SUCCESS = 0,
	SASLPREP_OOM = -1,			/* out of memory (only in frontend) */
	SASLPREP_INVALID_UTF8 = -2, /* input is not a valid UTF-8 string */
	SASLPREP_PROHIBITED = -3,	/* output would contain prohibited characters */
} pg_saslprep_rc;
```
## Detailed Description
The  enumeration provides standardized return codes for the PostgreSQL implementation of the SASLprep algorithm (RFC 4013), which is used to normalize user passwords during SCRAM authentication. SASLprep is a profile of the stringprep specification (RFC 3454) that prepares internationalized strings for use in authentication mechanisms.

The return codes indicate different outcomes of the password normalization process:
- Success when normalization completes without issues
- Memory allocation failures in frontend code
- Invalid UTF-8 input detection
- Detection of prohibited characters that would appear in the normalized output

This enumeration is used by both frontend (client) and backend code, making it a shared interface for SCRAM authentication across PostgreSQL components.

## Parameters / Member Variables
-  (0): Indicates successful completion of SASLprep normalization. The normalized password is available in the output parameter.
-  (-1): Out of memory error, occurring only in frontend code when malloc() fails during string allocation. Backend code uses palloc() which throws an ERROR instead.
-  (-2): The input string is not valid UTF-8. SASLprep requires valid UTF-8 input for proper Unicode normalization.
-  (-3): The normalized output would contain characters that are prohibited by the SASLprep specification, making the password unusable for SCRAM authentication.

## Dependencies
- Functions called/Symbols referenced:
  -  (the main function that returns this type)
- Called from (representative examples):
  -  (backend SCRAM secret generation)
  -  (backend password verification)
  -  (frontend SCRAM initialization)
  -  (frontend SCRAM secret generation)

## Notes and Other Information
- The enumeration is defined in  and used across both frontend and backend code
- Frontend code must handle  explicitly since malloc() can fail, while backend code uses PostgreSQL's memory management which handles out-of-memory via ereport(ERROR)
- When SASLprep normalization fails with  or , SCRAM authentication typically falls back to using the original password without normalization
- The negative values for error codes follow PostgreSQL conventions where success is 0 or positive, and errors are negative
- This type is essential for secure password handling in PostgreSQL's SCRAM-SHA-256 authentication mechanism, ensuring passwords are processed according to international standards for text preparation