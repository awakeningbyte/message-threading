This is a demo to exam a way of handling distributed, mostly ordered messages sent from different sources. 
It is suppose to restore sequential order and grouping messages with their Correlation ID.
    
    docker-compose up -d
    go test -benchmem  -bench .

### output
INFO[0000] benchmark setup                              
INFO[0000] setup workers                                
INFO[0000] generating 100000 messages in 100 groups     
INFO[0077] generated in 77.616646 seconds               
INFO[0077] start running benchmark    
                  
    total 100000 messages received
    goos: darwin
    goarch: amd64
    pkg: threader
    BenchmarkProcessThreads-8              1        59001100219 ns/op       218386432 B/op   3664403 allocs/op
PASS
INFO[0136] messages processed in 59.002582 seconds      
INFO[0136] checking output correctness                  
INFO[0136] Passed, total 100 groups, 100000 messages in output with correct sequential order 
ok      threader        136.704s
