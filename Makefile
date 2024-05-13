make:
	rm -rf datastore && mkdir datastore && go build ./... && go run ./cmd

run:
	./bitcask-go
