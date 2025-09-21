18.12. Registering Event Log on Windows  
---  
[Prev](ssh-tunnels.md "18.11. Secure TCP/IP Connections with SSH Tunnels") | [Up](runtime.md "Chapter 18. Server Setup and Operation")| Chapter 18. Server Setup and Operation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](runtime-config.md "Chapter 19. Server Configuration")  
  
* * *

## 18.12. Registering Event Log on Windows #

To register a Windows event log library with the operating system, issue this command: 
    
    
    **regsvr32 _pgsql_library_directory_ /pgevent.dll**
    

This creates registry entries used by the event viewer, under the default event source named `PostgreSQL`. 

To specify a different event source name (see [event_source](runtime-config-logging.md#GUC-EVENT-SOURCE)), use the `/n` and `/i` options: 
    
    
    **regsvr32 /n /i:_event_source_name_ _pgsql_library_directory_ /pgevent.dll**
    

To unregister the event log library from the operating system, issue this command: 
    
    
    **regsvr32 /u [/i:_event_source_name_] _pgsql_library_directory_ /pgevent.dll**
    

### Note

To enable event logging in the database server, modify [log_destination](runtime-config-logging.md#GUC-LOG-DESTINATION) to include `eventlog` in `postgresql.conf`. 

* * *

[Prev](ssh-tunnels.md "18.11. Secure TCP/IP Connections with SSH Tunnels") | [Up](runtime.md "Chapter 18. Server Setup and Operation")|  [Next](runtime-config.md "Chapter 19. Server Configuration")  
---|---|---  
18.11. Secure TCP/IP Connections with SSH Tunnels | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 19. Server Configuration
