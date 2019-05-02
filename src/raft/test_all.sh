#!/bin/bash
DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
cd $DIR

set -e

export TRAVIS_CI=1

./test.sh TestInitialElection2A
./test.sh TestReElection2A

./test.sh TestBasicAgree2B
./test.sh TestFailAgree2B
./test.sh TestFailNoAgree2B
./test.sh TestConcurrentStarts2B
./test.sh TestRejoin2B
./test.sh TestBackup2B
./test.sh TestCount2B

./test.sh TestPersist12C
./test.sh TestPersist22C
./test.sh TestPersist32C
./test.sh TestFigure82C
./test.sh TestUnreliableAgree2C
./test.sh TestFigure8Unreliable2C
./test.sh TestReliableChurn2C
./test.sh TestUnreliableChurn2C

