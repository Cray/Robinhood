export LUSTRE_NAME="lustre"
export LUSTRE_MNT="/mnt/lustre"

# Describes how to access first Lustre MDT as root (should not ask for password). For example:
# ssh root@mdt_node -oStrictHostKeyChecking=no
# Empty value means first Lustre MDT is in localhost.
export LUSTRE_MDT_COMMAND_SHELL=""

export HSMTOOL="lhsmtool_posix"
export HSM_ROOT="/tmp/hsm_root"
export TEST_DIR="tests"

export RBH_CONFIG_FILE="/local/home/devvm/config_rh/hsm.conf"
export RBH_LOG_FILE="/var/log/robinhood/lustre.log"
export RBH_DB="robinhood_lustre"

export N_ITERATIONS=1000
export DUMP_DIR="/tmp"
