#!/bin/bash
DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
cd $DIR

set -e

./test.sh TestInitialElection2A
./test.sh TestReElection2A

./test.sh TestBasicAgree2B
./test.sh TestFailAgree2B
./test.sh TestFailNoAgree2B
./test.sh TestConcurrentStarts2B
./test.sh TestRejoin2B
#./test.sh TestBackup2B
#./test.sh TestCount2B
