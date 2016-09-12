AM_CFLAGS= -static $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

# See autotools/m4/ax_valgrind_check.m4 for documentation
@VALGRIND_CHECK_RULES@

if ENABLE_UNIT_TESTS
noinst_PROGRAMS=ut
noinst_HEADERS=ut_test_suites.h

utdir=.
ut_SOURCES=ut_main.c \
	ut_changelog.c \
	ut_lustre_mock.c \
	ut_mysql_mock.c
ut_HEADERS=ut_test_suites.h
ut_CFLAGS=-iquote $(top_srcdir)/src/CUnit/CUnit/Headers $(AM_CFLAGS)
ut_LDFLAGS=-rdynamic -ldl $(AM_LDFLAGS)
ut_LDADD=$(top_srcdir)/src/CUnit/CUnit/Sources/libcunit.la \
	$(top_srcdir)/src/chglog_reader/libchglog_rd.la \
	$(top_srcdir)/src/entry_processor/libentryproc.la \
	$(top_srcdir)/src/cfg_parsing/librbhcfg.la \
	$(top_srcdir)/src/fs_scan/libfsscan.la \
	$(top_srcdir)/src/list_mgr/liblistmgr.la \
	$(top_srcdir)/src/policies/libpolicies.la \
	$(top_srcdir)/src/cfg_parsing/libconfigparsing.la \
	$(top_srcdir)/src/common/libcommontools.la
endif


indent:
	$(top_srcdir)/scripts/indent.sh
