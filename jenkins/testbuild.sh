#!/bin/bash

export BUILD_NUMBER=2
export JP_NEO_RELEASE=osaint
export JP_SCM_URL=http://es-gerrit.xyus.xyratex.com/robinhood
export JP_VERSION=3.0
export JP_NEO_ID="o.1.0"
export JP_REPO=robinhood

cd $(dirname $0)
cd ..
export WORKSPACE=$(pwd)

/bin/bash -x jenkins/build_phase2.sh
