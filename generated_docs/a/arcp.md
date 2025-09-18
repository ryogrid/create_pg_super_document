# arcp

## Location
src/backend/regex/regexec.c: 39 - 44

## Overview
The arcp struct represents a "pointer" to an outgoing arc in PostgreSQL's lazy-DFA (Deterministic Finite Automaton) representation for regular expression execution.

## Definition


## Detailed Description
The arcp structure is a core component of PostgreSQL's regular expression engine's lazy-DFA implementation. It serves as a lightweight representation of an arc (transition) in the DFA state machine, containing only the essential information needed to identify a specific outgoing transition from a state. The "lazy" aspect refers to the DFA's on-demand construction approach, where states and transitions are computed only when needed during pattern matching.

## Parameters / Member Variables
- Netid State   Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB   0      0                                               * 2456439               * 2456438          
u_str ESTAB   0      0                                               * 1998                  * 3655             
u_str ESTAB   0      0                                               * 189525                * 189524           
u_str ESTAB   0      0                                               * 2738760               * 2738759          
u_str ESTAB   0      0                                               * 186130                * 186129           
u_str ESTAB   0      0                                               * 184764                * 184765           
u_str ESTAB   0      0                                               * 4320555               * 4320554          
u_str ESTAB   0      0                                               * 175478                * 175479           
u_str ESTAB   0      0                                               * 2456438               * 2456439          
u_str ESTAB   0      0                                               * 186128                * 186127           
u_str ESTAB   0      0                                               * 184766                * 184767           
u_str ESTAB   0      0                                               * 10305                 * 10304            
u_str ESTAB   0      0                                               * 2738757               * 2738758          
u_str ESTAB   0      0                                               * 4320553               * 4320552          
u_str ESTAB   0      0                                               * 186127                * 186128           
u_str ESTAB   0      0                                               * 2738751               * 2738750          
u_str ESTAB   0      0                                               * 175477                * 175476           
u_str ESTAB   0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB   0      0                                               * 175479                * 175478           
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB   0      0                                               * 186126                * 186125           
u_str ESTAB   0      0                                               * 2456440               * 2456441          
u_str ESTAB   0      0                                               * 189528                * 189529           
u_str ESTAB   0      0                                               * 2456441               * 2456440          
u_str ESTAB   0      0                                               * 186125                * 186126           
u_str ESTAB   0      0                            /var/run/docker.sock 4296389               * 4307148          
u_str ESTAB   0      0                                               * 175483                * 175482           
u_str ESTAB   0      0                                               * 2738749               * 2738748          
u_str ESTAB   0      0                                               * 4320550               * 4320551          
u_str ESTAB   0      0                                               * 2738761               * 2738762          
u_str ESTAB   0      0                                               * 189531                * 189530           
u_str ESTAB   0      0                                               * 175480                * 175481           
u_str ESTAB   0      0                                               * 7569                  * 1891             
u_str ESTAB   0      0                                               * 4305451               * 4305452          
u_str ESTAB   0      0                                               * 11302                 * 11303            
u_str ESTAB   0      0                                               * 9317                  * 9318             
u_str ESTAB   0      0                                               * 189526                * 189527           
u_str ESTAB   0      0                                               * 10304                 * 10305            
u_str ESTAB   0      0                                               * 14343                 * 0                
u_str ESTAB   0      0                                               * 4307148               * 4296389          
u_str ESTAB   0      0                                               * 184765                * 184764           
u_str ESTAB   0      0                                               * 2738750               * 2738751          
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB   0      0                                               * 189529                * 189528           
u_str ESTAB   0      0                                               * 2456443               * 2456442          
u_str ESTAB   0      0                                               * 4305449               * 4305450          
u_str ESTAB   0      0                                               * 184767                * 184766           
u_str ESTAB   0      0                                               * 175482                * 175483           
u_str ESTAB   0      0                                               * 2738752               * 2738753          
u_str ESTAB   0      0                                               * 9318                  * 9317             
u_str ESTAB   0      0                                               * 175476                * 175477           
u_str ESTAB   0      0                                               * 189527                * 189526           
u_str ESTAB   0      0                                               * 4320554               * 4320555          
u_str ESTAB   0      0                                               * 4320552               * 4320553          
u_str ESTAB   0      0                                               * 2738758               * 2738757          
u_str ESTAB   0      0                                               * 189511                * 189512           
u_str ESTAB   0      0                                               * 2456437               * 2456436          
u_str ESTAB   0      0                                               * 2456442               * 2456443          
u_str ESTAB   0      0                                               * 2738753               * 2738752          
u_str ESTAB   0      0                                               * 3654                  * 10314            
u_str ESTAB   0      0                                               * 2456436               * 2456437          
u_str ESTAB   0      0                                               * 186129                * 186130           
u_str ESTAB   0      0                                               * 175481                * 175480           
u_str ESTAB   0      0                                               * 184769                * 184768           
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB   0      0                                               * 2738763               * 2738764          
u_str ESTAB   0      0                                               * 2738754               * 2738755          
u_str ESTAB   0      0                                               * 4305450               * 4305449          
u_str ESTAB   0      0                                               * 3614                  * 3615             
u_str ESTAB   0      0                                               * 184771                * 184770           
u_str ESTAB   0      0                                               * 11303                 * 11302            
u_str ESTAB   0      0                                               * 11300                 * 11301            
u_str ESTAB   0      0                                               * 4305452               * 4305451          
u_str ESTAB   0      0                                               * 189524                * 189525           
u_str ESTAB   0      0                                               * 189512                * 189511           
u_str ESTAB   0      0                                               * 2738764               * 2738763          
u_str ESTAB   0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB   0      0                                               * 3615                  * 3614             
u_str ESTAB   0      0                                               * 189530                * 189531           
u_str ESTAB   0      0                                               * 189504                * 189503           
u_str ESTAB   0      0                                               * 4320551               * 4320550          
u_str ESTAB   0      0                                               * 2738748               * 2738749          
u_str ESTAB   0      0                                               * 11301                 * 11300            
u_str ESTAB   0      0                                               * 3606                  * 15372            
u_str ESTAB   0      0                                               * 2738759               * 2738760          
u_str ESTAB   0      0                                               * 15398                 * 4219             
u_str ESTAB   0      0                                               * 189503                * 189504           
u_str ESTAB   0      0                                               * 184770                * 184771           
u_str ESTAB   0      0                                               * 184768                * 184769           
u_str ESTAB   0      0                                               * 2738755               * 2738754          
u_str ESTAB   0      0                                               * 2738762               * 2738761          
tcp   ESTAB   0      0                                  172.30.249.175:37848     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:58678         127.0.0.1:38351            
tcp   ESTAB   0      0                                  172.30.249.175:37850     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:38846     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:38430     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:38351         127.0.0.1:58678            
tcp   ESTAB   0      0                                       127.0.0.1:38351         127.0.0.1:58672            
tcp   ESTAB   0      0                                  172.30.249.175:52306     20.27.177.116:https            
tcp   ESTAB   0      0                                       127.0.0.1:58672         127.0.0.1:38351            
v_str ESTAB   0      0                                               *:633275402             2:50000            
v_str ESTAB   0      0                                               *:633275403             2:50000            
v_str ESTAB   0      0                                               *:633275404             2:50000            
v_str ESTAB   0      0                                               *:633275405             2:50000            
v_str ESTAB   0      0                                               *:633275406             2:50000            
v_str ESTAB   0      0                                               *:633275408             2:50001            
v_str ESTAB   0      0                                               *:633275409             2:50001            
v_str ESTAB   0      0                                               *:633275410             2:50001            
v_str ESTAB   0      0                                               *:633275424             2:50000            
v_str ESTAB   0      0                                               *:633275425             2:50000            
v_str ESTAB   0      0                                               *:633275426             2:50002            
v_str ESTAB   0      0                                               *:633275427             2:50002            
v_str ESTAB   0      0                                               *:633275428             2:50002            
v_str ESTAB   0      0                                               *:633275431             2:50002            
v_str ESTAB   0      0                                               *:633275432             2:50002            
v_str ESTAB   0      0                                               *:633275433             2:50002            
v_str ESTAB   0      0                                               *:1                     2:4102841729       
v_str ESTAB   0      0                                               *:633275411             2:4102841364       
v_str ESTAB   0      0                                               *:633275430             2:4102841703       
v_str ESTAB   0      0                                               *:633275430             2:4102841702       
v_str ESTAB   0      0                                               *:633275430             2:4102841701       
v_str CLOSING 0      0                                               *:633275430             2:4102841700       
v_str ESTAB   0      0                                               *:633275429             2:4102841697       
v_str ESTAB   0      0                                               *:633275435             2:4102841707       
v_str ESTAB   0      0                                               *:633275458             2:4102841830       
v_str ESTAB   0      0                                               *:633275458             2:4102841829       
v_str CLOSING 0      0                                               *:633275458             2:4102841828       
v_str CLOSING 0      0                                               *:633275458             2:4102841827       
v_str ESTAB   0      0                                               *:633275458             2:4102841826       
v_str ESTAB   0      0                                               *:633275457             2:4102841825       
v_str ESTAB   0      0                                               *:633275462             2:4102842074       
v_str ESTAB   0      0                                               *:633275462             2:4102842073       
v_str ESTAB   0      0                                               *:633275462             2:4102842072       
v_str ESTAB   0      0                                               *:633275462             2:4102842071       
v_str ESTAB   0      0                                               *:633275462             2:4102842070       
v_str ESTAB   0      0                                               *:633275463             2:4102842086       
v_str ESTAB   0      0                                               *:633275461             2:4102842069       
v_str ESTAB   0      0                                               *:633275466             2:4102842133       
v_str ESTAB   0      0                                               *:633275466             2:4102842132       
v_str ESTAB   0      0                                               *:633275466             2:4102842131       
v_str ESTAB   0      0                                               *:633275466             2:4102842130       
v_str ESTAB   0      0                                               *:633275466             2:4102842129       
v_str ESTAB   0      0                                               *:633275467             2:4102842156       
v_str ESTAB   0      0                                               *:633275464             2:4102842091       
v_str ESTAB   0      0                                               *:633275464             2:4102842090       
v_str ESTAB   0      0                                               *:633275464             2:4102842089       
v_str ESTAB   0      0                                               *:633275464             2:4102842088       
v_str ESTAB   0      0                                               *:633275464             2:4102842087       
v_str ESTAB   0      0                                               *:633275465             2:4102842128       
v_str ESTAB   0      0                                               *:633275470             2:4102842564       
v_str ESTAB   0      0                                               *:633275470             2:4102842563       
v_str ESTAB   0      0                                               *:633275470             2:4102842562       
v_str ESTAB   0      0                                               *:633275470             2:4102842561       
v_str ESTAB   0      0                                               *:633275470             2:4102842560       
v_str ESTAB   0      0                                               *:633275471             2:4102843465       
v_str ESTAB   0      0                                               *:633275468             2:4102842161       
v_str ESTAB   0      0                                               *:633275468             2:4102842160       
v_str ESTAB   0      0                                               *:633275468             2:4102842159       
v_str ESTAB   0      0                                               *:633275468             2:4102842158       
v_str ESTAB   0      0                                               *:633275468             2:4102842157       
v_str ESTAB   0      0                                               *:633275469             2:4102842559       
v_str ESTAB   0      0                                               *:633275475             2:4102843547       
v_str ESTAB   0      0                                               *:633275472             2:4102843470       
v_str ESTAB   0      0                                               *:633275472             2:4102843469       
v_str ESTAB   0      0                                               *:633275472             2:4102843468       
v_str ESTAB   0      0                                               *:633275472             2:4102843467       
v_str ESTAB   0      0                                               *:633275472             2:4102843466       
v_str ESTAB   0      0                                               *:633275478             2:4102843563       
v_str ESTAB   0      0                                               *:633275478             2:4102843562       
v_str ESTAB   0      0                                               *:633275478             2:4102843561       
v_str ESTAB   0      0                                               *:633275478             2:4102843560       
v_str ESTAB   0      0                                               *:633275478             2:4102843559       
v_str ESTAB   0      0                                               *:633275479             2:4102843569       
v_str ESTAB   0      0                                               *:633275476             2:4102843552       
v_str ESTAB   0      0                                               *:633275476             2:4102843551       
v_str ESTAB   0      0                                               *:633275476             2:4102843550       
v_str ESTAB   0      0                                               *:633275476             2:4102843549       
v_str ESTAB   0      0                                               *:633275476             2:4102843548       
v_str ESTAB   0      0                                               *:633275477             2:4102843558       
v_str ESTAB   0      0                                               *:633275480             2:4102843574       
v_str ESTAB   0      0                                               *:633275480             2:4102843573       
v_str ESTAB   0      0                                               *:633275480             2:4102843572       
v_str ESTAB   0      0                                               *:633275480             2:4102843571       
v_str ESTAB   0      0                                               *:633275480             2:4102843570       : Pointer to a state set (sset) representing the destination state of this arc
- : Color value representing the character class or symbol that triggers this transition

## Dependencies
- Functions called/Symbols referenced:
  - sset (destination state structure)
  - color (character class type)
- Called from (representative examples):
  - newdfa (DFA construction)
  - getvacant (state management)
  - NOPROGRESS (progress tracking)
  - dfa (DFA execution)
  - smalldfa (small DFA execution)

## Notes and Other Information
The arcp structure is designed for memory efficiency in the lazy-DFA implementation, storing only a state set pointer and color rather than full arc information. This allows the regex engine to minimize memory usage while maintaining the ability to traverse the automaton during pattern matching operations.