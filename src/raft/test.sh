#!/bin/bash
DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
cd $DIR
cd ../../
source env.sh
cd $DIR
go test -run $1
