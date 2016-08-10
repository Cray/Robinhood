SUBDIRS=include common list_mgr entry_processor fs_scan cfg_parsing modules policies

if CHANGELOGS
SUBDIRS+=chglog_reader
endif

SUBDIRS += robinhood tools tests

if ENABLE_UNIT_TESTS
SUBDIRS += CUnit ut ut_db
endif

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
