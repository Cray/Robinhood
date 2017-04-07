#!/bin/bash

#set -x
export PS4='+${BASH_SOURCE}:${FUNCNAME}:${LINENO}\n'

# Include test framework
. ./framework.sh

# TODO replace hard-coded sourcing with command line parsing
. ./config.sh

LUSTRE_MDT_NODE=${LUSTRE_MDT_NODE:-"localhost"}
DATAMOVER=${DATAMOVER:-"localhost"}

TESTS=${TESTS:-"dummy_success dummy_failure"}
declare -a TESTS_ARRAY
read -ra TESTS_ARRAY <<< $TESTS

FAILED_TESTS=""

declare -A TEST_HASH
# Keys of the TEST_HASH are TESTS_ARRAY array elements.
for key in "${!TESTS_ARRAY[@]}"; do TEST_HASH[${TESTS_ARRAY[$key]}]="$key"; done

check_lustre_mnt
[ $? -ne 0 ] && {
	echo "Lustre is not mounted in $LUSTRE_MNT or has wrong FS name"
	exit 1
}

set_changelog_mask
[ $? -ne 0 ] && {
	echo "Failed to set Lustre changelog mask"
	exit 1
}

hsm_enabled_status=$(do_cmd $LUSTRE_MDT_NODE lctl get_param mdt.$LUSTRE_NAME-MDT0000.hsm_control)
if [[ ! ("$hsm_enabled_status" =~ "enabled") ]]
then
	do_cmd $LUSTRE_MDT_NODE lctl set_param mdt.$LUSTRE_NAME-MDT0000.hsm_control=enabled
fi
[ $? -ne 0 ] && {
	echo "Failed to start HSM coordinator"
	exit 1
}

check_register_cl1
[ $? -ne 0 ] && exit 1

function dummy_success() {
	return 0
}

function dummy_failure() {
	return 1
}

function create_archive_move() {
	local test_file="$LUSTRE_MNT/$TEST_DIR/test_file$1"
	dd if=/dev/urandom of=$test_file bs=1k count=4 >& /dev/null
	lfs hsm_archive $test_file
	mv $test_file ${test_file}_1
}

function bm_create_archive_move() {
	run_rbh $@
	return $?
}

function create_archive_unlink() {
	local test_file="$LUSTRE_MNT/$TEST_DIR/test_file$1"
	dd if=/dev/urandom of=$test_file bs=1k count=4 >& /dev/null
	lfs hsm_archive $test_file
	rm -f $test_file
}

function bm_create_archive_unlink() {
	run_rbh $@
	return $?
}

function create_archive_release_mv_restore() {
	local test_file="$LUSTRE_MNT/$TEST_DIR/test_file$1"
	dd if=/dev/urandom of=$test_file bs=1k count=4 >& /dev/null
	lfs hsm_archive $test_file
	while [ -z "$(lfs hsm_state $test_file | grep 'archived')" ]
	do
	    sleep 1
	done
	lfs hsm_release $test_file
	while [ -z "$(lfs hsm_state $test_file | grep 'released')" ]
	do
	    sleep 1
	done
	mv $test_file ${test_file}_123
	lfs hsm_restore ${test_file}_123
	while [ -z "$(lfs hsm_state ${test_file}_123 | grep -v 'released')" ]
	do
	    sleep 1
	done
}

function bm_create_archive_release_mv_restore() {
	run_rbh $@
	return $?
}

function create_archive_release_unlink() {
	local test_file="$LUSTRE_MNT/$TEST_DIR/test_file$1"
	dd if=/dev/urandom of=$test_file bs=1k count=4 >& /dev/null
	lfs hsm_archive $test_file
	while [ -z "$(lfs hsm_state $test_file | grep 'archived')" ]
	do
	    sleep 1
	done
	lfs hsm_release $test_file
	while [ -z "$(lfs hsm_state $test_file | grep 'released')" ]
	do
	    sleep 1
	done
	rm -f $test_file
}

function bm_create_archive_release_unlink() {
	run_rbh $@
	return $?
}

benchmark bm_create_archive_move iterative_setup dump_db $N_ITERATIONS create_archive_move
benchmark bm_create_archive_unlink iterative_setup dump_db $N_ITERATIONS create_archive_unlink
benchmark bm_create_archive_release_mv_restore iterative_setup_copytool dump_db $N_ITERATIONS create_archive_release_mv_restore
benchmark bm_create_archive_release_unlink iterative_setup_copytool dump_db $N_ITERATIONS create_archive_release_unlink

run_test dummy_success changelog_cleanup changelog_cleanup 1 dummy_success
run_test dummy_failure changelog_cleanup changelog_cleanup 1 dummy_failure

if [ -n "$FAILED_TESTS" ]
then
	echo "Failed tests:$FAILED_TESTS"
	exit 1
fi


