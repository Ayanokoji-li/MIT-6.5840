#!/usr/bin/bash

while true; do
    VERBOSE=2 go test -run TestInitialElection3A || break
    VERBOSE=2 go test -run TestReElection3A || break
    VERBOSE=2 go test -run TestManyElections3A || break
done