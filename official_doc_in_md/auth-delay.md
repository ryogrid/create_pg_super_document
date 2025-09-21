F.2. auth_delay — pause on authentication failure  
---  
[Prev](amcheck.md "F.1. amcheck — tools to verify table and index consistency") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](auto-explain.md "F.3. auto_explain — log execution plans of slow queries")  
  
* * *

## F.2. auth_delay — pause on authentication failure #

[F.2.1. Configuration Parameters](auth-delay.md#AUTH-DELAY-CONFIGURATION-PARAMETERS)
[F.2.2. Author](auth-delay.md#AUTH-DELAY-AUTHOR)

`auth_delay` causes the server to pause briefly before reporting authentication failure, to make brute-force attacks on database passwords more difficult. Note that it does nothing to prevent denial-of-service attacks, and may even exacerbate them, since processes that are waiting before reporting authentication failure will still consume connection slots. 

In order to function, this module must be loaded via [shared_preload_libraries](runtime-config-client.md#GUC-SHARED-PRELOAD-LIBRARIES) in `postgresql.conf`. 

### F.2.1. Configuration Parameters #

`auth_delay.milliseconds` (`integer`) 
    

The number of milliseconds to wait before reporting an authentication failure. The default is 0. 

These parameters must be set in `postgresql.conf`. Typical usage might be: 
    
    
    # postgresql.conf
    shared_preload_libraries = 'auth_delay'
    
    auth_delay.milliseconds = '500'
    

### F.2.2. Author #

KaiGai Kohei `<[kaigai@ak.jp.nec.com](mailto:kaigai@ak.jp.nec.com)>`

* * *

[Prev](amcheck.md "F.1. amcheck — tools to verify table and index consistency") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](auto-explain.md "F.3. auto_explain — log execution plans of slow queries")  
---|---|---  
F.1. amcheck — tools to verify table and index consistency | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.3. auto_explain — log execution plans of slow queries
