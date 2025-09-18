# CheckMD5Auth

## Location
[src/backend/libpq/auth.c:890-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L890-L927)

## Overview
CheckMD5Auth implements MD5 challenge-response authentication for PostgreSQL, generating a random salt and verifying the client's MD5-hashed password response.

## Definition


## Detailed Description
CheckMD5Auth performs MD5-based challenge-response authentication by generating a cryptographically secure 4-byte random salt, sending it to the client along with an MD5 authentication request, and then verifying the client's hashed response against the stored password hash. This method prevents plaintext passwords from being transmitted over the network while providing authentication verification. The function handles the complete MD5 authentication flow including salt generation, client communication, and password verification.

## Parameters / Member Variables
- : Connection port structure containing client connection information and user details
- : Pre-retrieved password hash for the user from the database
- : Output parameter for detailed error messages that can be included in authentication logs

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strong_random](../p/pg_strong_random.md) (generates cryptographically secure random salt)
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends AUTH_REQ_MD5 request with salt to client)
  - [recv_password_packet](../r/recv_password_packet.md) (receives MD5 hashed response from client)
  - [md5_crypt_verify](../m/md5_crypt_verify.md) (verifies client's MD5 response against stored hash)
  - [pfree](../p/pfree.md) (memory cleanup for received password)
- Called from (representative examples):
  - [CheckPWChallengeAuth](CheckPWChallengeAuth.md) function in auth.c:867

## Notes and Other Information
- Uses 4-byte random salt to prevent rainbow table attacks
- Returns STATUS_ERROR if random salt generation fails or if no shadow_pass provided
- Returns STATUS_EOF if client doesn't send password response
- Returns result of md5_crypt_verify on successful processing
- Part of the challenge-response authentication mechanisms, more secure than plaintext passwords
- Always cleans up received password data from memory
- Relies on CheckPWChallengeAuth for higher-level authentication logic and identity setting