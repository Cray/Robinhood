#!/bin/bash

PS4='+$BASH_SOURCE:$FUNCNAME:$LINENO:\n'
#set -x

. config.sh

. functions.sh

function test_action() {
	local number=$1

	local test_file="$LUSTRE_MNT"/"$TEST_DIR"/test_file_$number
	cp /etc/passwd "$test_file"
	lfs hsm_archive "$test_file"
	while [ -z "$(lfs hsm_state "$test_file" | grep 'archived')" ]
	do
	    sleep 1
	done
	lfs hsm_release "$test_file"
	while [ -z "$(lfs hsm_state "$test_file" | grep 'released')" ]
	do
	    sleep 1
	done
	rm -f "$test_file"
}

function test_action_setup() {
	local hsm_root=${HSM_ROOT:-"/tmp/arc1"}
	local lustre_dir=${LUSTRE_MNT:-"/mnt/lustre"}

	killall $HSMTOOL

	mkdir -p $hsm_root $lustre_dir/$TEST_DIR
	find $lustre_dir/$TEST_DIR | grep -v "^$lustre_dir/$TEST_DIR[/]\?$" | xargs rm -rf 
	$HSMTOOL -p $hsm_root $LUSTRE_MNT >& /dev/null &
}

function test_action_cleanup() {
	local hsm_root=${HSM_ROOT:-"/tmp/arc1"}
	local lustre_dir=${LUSTRE_MNT:-"/mnt/lustre"}

	killall $HSMTOOL
	find $hsm_root | grep -v "^$hsm_root[/]\?$" | xargs rm -rf 
	find $lustre_dir/$TEST_DIR | grep -v "^$lustre_dir/$TEST_DIR[/]\?$" | xargs rm -rf 
}

env_setup

echo "$$"

collapsing_check action
