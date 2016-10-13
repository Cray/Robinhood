#!/bin/bash

PS4='+$BASH_SOURCE:$FUNCNAME:$LINENO:\n'
#set -x

. config.sh

. functions.sh

function test_action() {
	local number=$1

	local test_dir="$LUSTRE_MNT"/"$TEST_DIR"
	cp /etc/passwd "$test_dir"/test_file_$number
	#lfs hsm_archive /mnt/lustre/passwd
	mv "$test_dir"/test_file_$number "$test_dir"/test_file_${number}.new
	rm -f "$test_dir"/test_file_${number}.new
}

env_setup

echo "$$"
collapsing_check action
