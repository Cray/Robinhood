AM_CFLAGS= -pthread $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread -rpath $(pkglibdir)

# See autotools/m4/ax_valgrind_check.m4 for documentation
@VALGRIND_CHECK_RULES@

if ENABLE_UNIT_TESTS
noinst_PROGRAMS=ut_db st_lmgr
noinst_HEADERS=ut_test_suites.h tests_lmgr_common.h

all_libs = \
	$(top_srcdir)/src/entry_processor/libentryproc.la \
	$(top_srcdir)/src/policies/libpolicies.la \
	$(top_srcdir)/src/cfg_parsing/librbhcfg.la \
	$(top_srcdir)/src/chglog_reader/libchglog_rd.la \
	$(top_srcdir)/src/fs_scan/libfsscan.la \
	$(top_srcdir)/src/common/libcommontools.la \
	$(top_srcdir)/src/list_mgr/liblistmgr.la \
	$(top_srcdir)/src/CUnit/CUnit/Sources/libcunit.la \
	$(top_srcdir)/src/cfg_parsing/libconfigparsing.la

ut_dbdir=.
ut_db_SOURCES=ut_main.c \
	ut_listmanager.c \
	tests_lmgr_common.c \
	ut_lustre_mock.c
ut_db_HEADERS=ut_test_suites.h tests_lmgr_common.h
ut_db_CFLAGS=-iquote $(top_srcdir)/src/CUnit/CUnit/Headers $(AM_CFLAGS) \
	-iquote $(top_srcdir)/src/list_mgr
ut_db_LDFLAGS=-rdynamic -ldl $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

st_lmgrdir=.
st_lmgr_SOURCES=st_main.c \
	tests_lmgr_common.c \
	ut_lustre_mock.c
st_lmgr_HEADERS=tests_lmgr_common.h
st_lmgr_CFLAGS= -iquote $(top_srcdir)/src/list_mgr $(AM_CFLAGS)
st_lmgr_LDFLAGS=-rdynamic -ldl $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

endif ENABLE_UNIT_TESTS


indent:
	$(top_srcdir)/scripts/indent.sh
