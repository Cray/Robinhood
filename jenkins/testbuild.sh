#!/bin/bash
# you don't actually need to use this script -- just run the
# command at the end.

# Since build does git archive you will have to commit before
# running the test build.
export BUILD_NUMBER=2
export JP_NEO_RELEASE=osaint
export JP_SCM_URL=http://es-gerrit.xyus.xyratex.com/robinhood
export JP_VERSION=3.0
export JP_NEO_ID="x.y.z"
export JP_REPO=robinhood

cd $(dirname $0)
cd ..
export WORKSPACE=$(pwd)

/bin/bash -x jenkins/build_phase2.sh
