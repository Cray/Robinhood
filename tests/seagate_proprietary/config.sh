# Lustre filesystem name.
LUSTRE_NAME="lustre"
# Lustre mount point.
LUSTRE_MNT=/mnt/lustre
# MDT node with HSM coordinator.
# Either empty value or "localhost" string will result in pure local command execution.
# Will require passwordless ssh access otherwise.
LUSTRE_MDT_NODE=
# HSM copytool name
HSMTOOL_NAME="lhsmtool_posix"
# HSM copytool command line parameters to use.
HSMTOOL_CMD_PARAMS="-p /tmp/hsm_backend /mnt/lustre"
# Data mover node DNS name or IP address.
# Either empty value or "localhost" string will result in pure local command execution.
# Will require passwordless ssh access otherwise.
DATAMOVER=
# Lustre directory for test files - relative to Lustre mount point.
TEST_DIR=sgt_tests
# Robinhood configuration file to use. Test framework will make changes to it.
RBH_CONFIG_FILE=/local/home/devvm/etc/robinhood.d/hsm.conf
# Robinhood log file as per configuration file.
RBH_LOG_FILE=/var/log/robinhood/lustre.log
# Robinhood database (same as in configuration file). Test framework will clean it while running.
RBH_DB=robinhood_lustre
# Number of action iterations per test.
N_ITERATIONS=100
# Absolute path to directory to put dump files for temporary storage. 
DUMP_DIR=/tmp/sgt_tmp_dumps

