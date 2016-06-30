AM_CFLAGS= -static $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

# See autotools/m4/ax_valgrind_check.m4 for documentation
@VALGRIND_CHECK_RULES@

if ENABLE_UNIT_TESTS
noinst_PROGRAMS=ut
noinst_HEADERS=ut_test_suites.h

ut_SOURCES=ut_main.c
ut_CFLAGS=-iquote $(top_srcdir)/src/CUnit/CUnit/Headers $(AM_CFLAGS)
ut_LDADD=$(top_srcdir)/src/CUnit/CUnit/Sources/libcunit.la
endif


indent:
	$(top_srcdir)/scripts/indent.sh
