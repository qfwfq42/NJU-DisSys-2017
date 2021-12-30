go test -run InitialElection
go test -run ReElection
go test -run BasicAgree
go test -run FailAgree
go test -run FailNoAgree
go test -run ConcurrentStarts
go test -run Rejoin
go test -run Backup
go test -run Count
go test -run Persist1
go test -run Persist2
go test -run Persist3
go test -run UnreliableAgree
go test -run ReliableChurn
go test -run UnreliableChurn
go test -run Figure8
sleep 100