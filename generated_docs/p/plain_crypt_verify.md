Documentation for plain_crypt_verify symbol

## Simplified Source

```c
int plain_crypt_verify(const char *role, const char *shadow_pass,
                      const char *client_pass, const char **logdetail)
{
    char crypt_client_pass[MD5_PASSWD_LEN + 1];
    const char *errstr = NULL;

    // Determine password type and verify accordingly
    switch (get_password_type(shadow_pass))
    {
        case PASSWORD_TYPE_SCRAM_SHA_256:
            // Use SCRAM verification for SCRAM passwords
            if (scram_verify_plain_password(role, client_pass, shadow_pass))
                return STATUS_OK;
            else
            {
                *logdetail = psprintf("Password does not match for user \"%s\".", role);
                return STATUS_ERROR;
            }

        case PASSWORD_TYPE_MD5:
            // Hash client password with MD5 and compare
            if (!pg_md5_encrypt(client_pass, role, strlen(role),
                               crypt_client_pass, &errstr))
            {
                *logdetail = errstr;
                return STATUS_ERROR;
            }

            if (strcmp(crypt_client_pass, shadow_pass) == 0)
                return STATUS_OK;
            else
            {
                *logdetail = psprintf("Password does not match for user \"%s\".", role);
                return STATUS_ERROR;
            }

        case PASSWORD_TYPE_PLAINTEXT:
            // Plaintext passwords are not stored - should not happen
            break;
    }

    // Unrecognized password format
    *logdetail = psprintf("Password of user \"%s\" is in unrecognized format.", role);
    return STATUS_ERROR;
}
```