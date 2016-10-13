function env_setup() {
	local CHGLOG_MASK=$($LUSTRE_MDT_COMMAND_SHELL lctl get_param mdd.$LUSTRE_NAME-MDT0000.changelog_mask)
        local EVENT_LIST="HSM CREAT UNLNK TRUNC SATTR CTIME MTIME CLOSE RENME RNMTO RMDIR HLINK LYOUT"
	for event in $EVENT_LIST
	do
		if ! $(echo "$CHGLOG_MASK" | grep -q $event)
		then
			$LUSTRE_MDT_COMMAND_SHELL lctl set_param mdd.$LUSTRE_NAME-MDT0000.changelog_mask="+$event"
		fi
	done
	local changelog_clients=$($LUSTRE_MDT_COMMAND_SHELL lctl get_param mdd.$LUSTRE_NAME-MDT0000.changelog_users)
	if ! $(echo "$changelog_clients" | grep -q cl1)
	then
		$LUSTRE_MDT_COMMAND_SHELL lctl changelog_register --device $LUSTRE_NAME-MDT0000 -n
	fi

	if ! $($LUSTRE_MDT_COMMAND_SHELL lctl get_param mdt.$LUSTRE_NAME-MDT0000.hsm_control | grep -q enabled)
	then
		$LUSTRE_MDT_COMMAND_SHELL lctl set_param mdt.$LUSTRE_NAME-MDT0000.hsm_control=enabled
	fi
}

function test_setup() {
	local cfg_file=${RBH_CONFIG_FILE:-"/etc/robinhood.d/hsm.conf"}
	local guard_file="$LUSTRE_MNT"/"$TEST_DIR"/guard
	mkdir -p "$LUSTRE_MNT"/"$TEST_DIR"
	touch "$guard_file"
	lfs changelog_clear $LUSTRE_NAME-MDT0000 cl1 0
	rbh-config empty_db "$RBH_DB" >& /dev/null
	robinhood --scan --once -f "$cfg_file" >& /dev/null
	lfs path2fid "$guard_file" | sed 's/\(\[\|\]\)//g'
}

function rbh_start() {
	local cfg_file=${RBH_CONFIG_FILE:-"/etc/robinhood.d/hsm.conf"}
	local DUMP_DIR=${DUMP_DIR:-"/tmp"}
	local compacting=${1:-"yes"}
	local rbh_pid

	if $(grep -q '^[^#]*compacting[^#]*=' "$RBH_CONFIG_FILE")
	then
		sed -i "s/^\([^#]*compacting\).*/\1 = $compacting;/" "$RBH_CONFIG_FILE"
	else
		sed -i "s/^\([^#]*ChangeLog[^#]{\).*/\1\ncompacting = $compacting;/" "$RBH_CONFIG_FILE"
	fi
	robinhood --readlog --run=all -f "$cfg_file" >& "$DUMP_DIR"/$$.rbh.${compacting}.out &
	echo $!
}

function rbh_wait_kill {
	local rbh_pid=$1
	local guard_fid=$2

	[ -z "$rbh_pid" -o -z "$guard_fid" ] && return 255

	while $(lfs changelog $LUSTRE_NAME-MDT0000 | grep UNLNK | grep -q $guard_fid)
	do
		sleep 1
	done
	echo $SECONDS

	kill $rbh_pid
	sleep 10
}

function test_action_run() {
	local test_name=$1
	local collapsing=${2:-"yes"}
	local n_iterations=${N_ITERATIONS:-5000}
	local iteration=0
	local dump_file_name="$DUMP_DIR"/$$.${collapsing}.dump

	[ -z "$test_name" ] && return 255

	if [ $(type -t "test_${test_name}_setup") == "function" ]
	then
		test_${test_name}_setup
	fi

	local guard_fid=$(test_setup)

	while [ $iteration -lt $n_iterations ]
	do
		iteration=$(($iteration + 1))
		test_$test_name $iteration
		if [ $(($iteration % 5000)) -eq 0 ]
		then
			echo "Ran iteration $iteration of $n_iterations for test '$test_name', collapsing: '$collapsing''"
		fi
	done

	rm -f "$LUSTRE_MNT"/"$TEST_DIR"/guard

	while ! $(lfs changelog $LUSTRE_NAME-MDT0000 | grep -q $guard_fid)
	do
		sleep 1
	done

	echo "MySQL status before robinhood started:"
	mysql -e "status" $RBH_DB

	local rbh_pid=$(rbh_start $collapsing)
	local rbh_started=$SECONDS

	if [ "$(pgrep robinhood)" != "$rbh_pid" ]
	then
		echo "Failed to start robinhood"
		return 255
	fi
	local rbh_finished=$(rbh_wait_kill $rbh_pid $guard_fid)

	echo "MySQL status after robinhood finished:"
	mysql -e "status" $RBH_DB

	echo "Robinhood ran for $(($rbh_finished - $rbh_started)) seconds"

	mysql --batch -e "SELECT owner, gr_name, size, blocks, type, mode, nlink, invalid, fileclass, lhsm_status, lhsm_archid, lhsm_norels, lhsm_noarch, lhsm_lstarc, lhsm_lstrst from ENTRIES order by id" "$RBH_DB" > "$dump_file_name"
	mysql --batch -e "select name from NAMES order by id" "$RBH_DB" >> "$DUMP_DIR"/$$.${collapsing}.dump
	mysql --batch -e "SELECT validator, stripe_count, stripe_size, pool_name from STRIPE_INFO order by id" "$RBH_DB" >> "$dump_file_name"
	mysql --batch -e "SELECT stripe_index from STRIPE_ITEMS order by id" "$RBH_DB" >> "$dump_file_name"

	if [ $(type -t "test_${test_name}_cleanup") == "function" ]
	then
		test_${test_name}_cleanup
	fi
}

function collapsing_check() {
	local test_name=$1
	local cleanup_after_test=${2:-"yes"}

	local n_iterations=${N_ITERATIONS:-5000}
	local test_path="$LUSTRE_MNT/$TEST_DIR"

	mkdir -p "$test_path"
	local rbh_ran_compacting=$(test_action_run $test_name "yes")
	[ "$cleanup_after_test" == "yes" ] && rm -rf "$test_path"
	mkdir -p "$test_path"
	local rbh_ran_no_compacting=$(test_action_run $test_name "no")
	[ "$cleanup_after_test" == "yes" ] && rm -rf "$test_path"

	echo "Stats for $test_name:"
	echo "    with in-memory DB collapsing enabled:"
	echo "$rbh_ran_compacting"
	echo
	echo "    without in-memory DB collapsing enabled:"
	echo "$rbh_ran_no_compacting"
	echo
	echo -n "Robinhood DBs with and without collapsing are "
	if $(cmp -s "$DUMP_DIR"/$$.no.dump "$DUMP_DIR"/$$.yes.dump)
	then
		echo "same"
	else
		echo "different"
	fi
	echo ""
}
