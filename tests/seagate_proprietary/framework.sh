function do_cmd() {
	local host=$1
	shift
	[ -z "$1" ] && return

	if [ "$host" == "localhost" ]
	then
		bash -c "$*"
	else
		ssh "$host" "$*"
	fi
}

function set_changelog_mask() {
	local events="HSM CREAT UNLNK TRUNC SATTR CTIME MTIME CLOSE RENME RNMTO RMDIR HLINK LYOUT"

	for event in $events
       	do
		do_cmd $LUSTRE_MDT_NODE "lctl set_param -P mdd.$LUSTRE_NAME-MDT0000.changelog_mask +$event"
		local result=$?
		[ $result -ne 0 ] && return $result
	done
	return 0
}

function check_lustre_mnt() {
	mount -t lustre | grep ":/$LUSTRE_NAME" | grep -q "$LUSTRE_MNT"
}

function check_register_cl1() {
	local users=$(do_cmd $LUSTRE_MDT_NODE "lctl get_param mdd.$LUSTRE_NAME-MDT0000.changelog_users")
	[ $? -ne 0 ] && {
		echo "Failed to query changelog client list"
		return 1
	}

	local has_cl1=$(echo "$users" | grep -c 'cl1\>')
	if [ "$has_cl1" == "0" ]
	then
		do_cmd $LUSTRE_MDT_NODE \
			"lctl --device $LUSTRE_NAME-MDT0000 changelog_register"
		[ $? -ne 0 ] && {
			echo "Failed to register client"
			return 1
		}
		users=$(do_cmd $LUSTRE_MDT_NODE "lctl get_param mdd.$LUSTRE_NAME-MDT0000.changelog_users")
		[ $? -ne 0 ] && {
			echo "Failed to query changelog client list after" \
				"client registration"
			return 1
		}
		has_cl1=$(echo "$users" | grep -q 'cl1\>')
		[ "$has_cl1" == 0 ] && {
			echo "Registered client other than cl1. Please cleanup."
			return 1
		}
	fi

	return 0
}

function is_shell_fnc() {
	local fnc_name="$1"
	[ -z "$fnc_name" ] && {
		echo "Empty function name."
		return 1
	}
	type "$fnc_name" | grep -q "function"
}

# Run a benchmark test
# Arguments:
# Test function name - mandatory, must be defined.
# Setup function name - test data generation function. Mandatory
# Teardown function name - mandatory.
# Action name - mandatory.
# Number of iterations - number of test setup cycles. Will be passed to test setup function.
# Other parameters for test setup function.
function run_test() {
#TODO fail test if setup or following not set
	local test_name="$1"
	is_shell_fnc "$test_name"
	[ $? -ne 0 ] && return 1
	# We don't want to run this test
	[[ -z "${TEST_HASH[$test_name]}" ]] && return 0
	shift
	local setup_name="$1"
	$(is_shell_fnc "$setup_name") || return 1
	shift
	local teardown_name="$1"
	$(is_shell_fnc "$teardown_name") || return 1
	shift
	local iterations="$1"
	[ -z "$iterations" ] && {
		echo "No iterations provided"
		return 1
	}
	shift
	local action_name="$1"

	local dump_dir=$DUMP_DIR/$action_name
	mkdir -p $dump_dir

	echo -n "$test_name... "
	$setup_name "$iterations" "$@"
	[ $? -ne 0 ] && {
		fail $test_name "Setting up test '$test_name' failed."
		return 1
	}

	set_cfg_collapsing $USE_COLLAPSING

	local questions_before=$(mysql $RBH_DB <<< status | grep Questions | \
		sed -e "s/ \+/ /g" | cut -d' ' -f4-4 )
	local TEST_START=$SECONDS
	$($test_name "$iterations" $@)
	local result=$?
	local TEST_FINISH=SECONDS
	local questions_after=$(mysql $RBH_DB <<< status | grep Questions | \
		sed -e "s/ \+/ /g" | cut -d' ' -f4-4 )

	if [ $result -eq 0 ]
	then
		echo -n "SUCCESS with"
		[ $USE_COLLAPSING -eq 0 ] && echo -n "out"
		echo -n " collapsing after $(($TEST_FINISH - $TEST_START)) seconds"
		echo "with $questions_before queries before test and" \
			"$questions_after after test"
	else
		fail $test_name "test action $test_name failed"
		echo -n "FAILURE with"
		[ $USE_COLLAPSING -eq 0 ] && echo -n "out"
		echo " collapsing"
	fi

	$teardown_name "$iterations" $@
	[ $? -ne 0 ] && {
		fail $test_name "Cleaning up after test '$test_name' failed"
		return 2
	}

	return 0
}

function changelog_cleanup() {
	lfs changelog_clear $LUSTRE_NAME-MDT0000 cl1 0
}

function start_robinhood() {
	#local log_file=${1:-"/dev/null"}
	local log_file="/dev/null"
	shift

	robinhood --detach --log-file=$log_file -f $RBH_CONFIG_FILE $* \
		< /dev/null >& $log_file
	pgrep robinhood
}

function stop_process() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo "Cannot stop empty PID."
		return 1
	}
	local timeout=${2:-5}
	local host=${3:-localhost}

	do_cmd $host kill $pid >& /dev/null
	# We don't want to output anything here. It is caller's duty to decide.
	[ $? -eq 0 ] || return 2

	sleep $timeout
	do_cmd $host kill -s 0 $pid >& /dev/null
	[ $? -eq 0 ] && return 3
	return 0
}

function set_cfg_collapsing() {
	local line_num=$(grep -n '^\s*name\s*=\s*"collapse"' $RBH_CONFIG_FILE | \
		cut -d':' -f1-1)
	[ $? -ne 0 ] && {
		echo "Failed to change collapsing status"
		return 1
	}
	if [[ $USE_COLLAPSING -eq 0 ]]
	then
		sed -i "$line_num,+1 s/yes/no/" $RBH_CONFIG_FILE
	else
		sed -i "$line_num,+1 s/no/yes/" $RBH_CONFIG_FILE
	fi
	return $?
}

function iterative_setup() {
	local iterations="$1"
	shift
	local setup_action="$1"
	$(is_shell_fnc "$setup_action") || {
		echo "'$setup_action' is not a function"
		return 1
	}
	shift

	local dump_dir=$DUMP_DIR/$setup_action
	mkdir -p $dump_dir

	local test_dir=$LUSTRE_MNT/$TEST_DIR
	rm -rf $test_dir
	mkdir -p $test_dir

	rbh-config empty_db $RBH_DB >& /dev/null
	touch $test_dir/lustre_guard

	local pid=$(start_robinhood /dev/null --scan=once)
	wait $pid >& /dev/null

	changelog_cleanup
	sleep 5

	local n=0
	echo "" >&2
	while [ $n -lt $iterations ]
	do
		$setup_action $n
		[ $? -ne 0 ] && return 1
		n=$(($n + 1))
		[[ $(($n % 50)) -eq 0 ]] && \
		       	echo "Setup iteration $n of $iterations" >&2
	done

	rm -f $LUSTRE_MNT/$TEST_DIR/lustre_guard

	return 0
}

function iterative_setup_copytool() {
	local iterations="$1"
	shift
	local setup_action="$1"
	$(is_shell_fnc "$setup_action") || {
		echo "'$setup_action' is not a function"
		return 1
	}
	shift

	local dump_dir=$DUMP_DIR/$setup_action
	mkdir -p $dump_dir

	local test_dir=$LUSTRE_MNT/$TEST_DIR
	rm -rf $test_dir
	mkdir -p $test_dir

	rbh-config empty_db $RBH_DB >& /dev/null
	touch $test_dir/lustre_guard

	local pid=$(start_robinhood /dev/null --scan=once)
	wait $pid >& /dev/null

	changelog_cleanup
	sleep 5

	local hsm_pid=$(start_copytool $test_name)
	[[ $? -ne 0 || -z "$hsm_pid"  ]] && return 1

	local n=0
	echo "" >&2
	while [ $n -lt $iterations ]
	do
		$setup_action $n
		[ $? -ne 0 ] && return 1
		n=$(($n + 1))
		[[ $(($n % 50)) -eq 0 ]] && \
		       	echo "Setup iteration $n of $iterations" >&2
	done

	rm -f $LUSTRE_MNT/$TEST_DIR/lustre_guard

	stop_process $hsm_pid 60 $DATAMOVER

	return 0
}

function dump_db() {
	local dump_file="$DUMP_DIR/$2/rbh.${1}iter.with"
	if [[ $USE_COLLAPSING -eq 0 ]]
	then
		dump_file="${dump_file}out"
	fi
	dump_file="${dump_file}_collapsing.dump"

	#mysqldump $RBH_DB > $dump_file
}

function benchmark() {
	USE_COLLAPSING=0
	run_test $@
	USE_COLLAPSING=1
	run_test $@
}

function run_rbh() {
	local log_file="$DUMP_DIR/$2/rbh.${1}iter.with"
	if [ $USE_COLLAPSING -eq 0 ]
	then
		log_file="${log_file}out"
	fi
	log_file="${log_file}_collapsing.log"

	local ulimit_core=$(ulimit -c)
	ulimit -c unlimited

	local rbh_pid=$(start_robinhood $log_file --readlog --run=all)

	while $(lfs changelog $LUSTRE_NAME-MDT0000 | grep -q UNLNK)
	do
		if ! $(check_process $rbh_pid $LUSTRE_MDT_NODE)
		then
			ulimit -c $ulimit_core
			if [ -f "core.$rbh_pid" ]
			then
				mv core.$rbh_pid $(dirname $log_file)
			fi
			return 1
		fi
		sleep 1
	done

	stop_process $rbh_pid
	local result=$?

	ulimit -c $ulimit_core

	case $result in
		2)
			if [ -f "core.$rbh_pid" ]
			then
				mv core.$rbh_pid $(dirname $log_file)
			fi
			;;
		1 | 3)
			fail $2 "Failed to stop robinhood"
			;;
		*)
			;;
	esac

	return $result
}

function check_process() {
	local pid=$1
	local node=${2:-"localhost"}

	kill -0 $pid >& /dev/null
	return $?
}

function start_copytool() {
	$(do_cmd $DATAMOVER "$HSMTOOL_NAME --daemon $HSMTOOL_CMD_PARAMS < /dev/null >& /dev/null")
	[ $? -eq 0 ] || {
		fail $1 "Failed to start copytool in $DATAMOVER"
		return 1
	}
	local copytool_pid=$(do_cmd $DATAMOVER pgrep $HSMTOOL_NAME)
	[ $? -eq 0 ] || {
		fail $1 "Copytool didn't start in $DATAMOVER"
		return 2
	}
	echo $copytool_pid
}

function fail() {
	local test_name=$1
	shift

	echo -n "FAILURE with"
	[ $USE_COLLAPSING -eq 0 ] && echo -n "out"
	echo " collapsing"
	echo "${FUNCNAME[@]}" >2
	echo $@ >2
	FAILED_TESTS="$FAILED_TESTS $test_name"
}
