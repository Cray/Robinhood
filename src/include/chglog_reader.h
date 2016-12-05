/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file    chglog_reader.h
 * \author  Th. Leibovici
 * \brief   Interface for Lustre MDT Changelog processing.
 */

/**
 * \addtogroup CHANGE_LOGS
 * @{
 */
#ifndef _CHGLOG_READER_H
#define _CHGLOG_READER_H

#include "config_parsing.h"
#include "rbh_const.h"
#include "list_mgr.h"
#include "list.h"
#include "lustre_extended_types.h"
#include <stdbool.h>
#include "entry_proc_hash.h"
#include "chglog_postproc.h"

#define MDT_NAME_MAX  32
#define READER_ID_MAX 16

/* Number of entries in each readers' op hash table. */
#define ID_CHGLOG_HASH_SIZE 7919

typedef struct mdt_def_t {
    char mdt_name[MDT_NAME_MAX];
    char reader_id[READER_ID_MAX];
} mdt_def_t;

/** Configuration for ChangeLog reader Module */
typedef struct chglog_reader_config_t {
    /** List of MDTs (used for opening ChangeLogs) */
    mdt_def_t *mdt_def;
    unsigned int mdt_count;

    /** List of post-processor instances */
    cpp_instance_t **cppi_def;
    unsigned int    cppi_count;

    /* nbr of changelog records to be agregated for llapi_changelog_clear() */
    int batch_ack_count;

    bool force_polling;
    time_t polling_interval;

    /* Maximum number of operations to keep in the internal queue. */
    int queue_max_size;

    /* Age of the opration we keep in the internal queue before we
     * push them to thepipeline. */
    time_t queue_max_age;

    /* Interval at which we have to check whether operation in the
     * internal queue have aged. */
    time_t queue_check_interval;

    /* Options suported by the MDS. LU-543 and LU-1331 are related to
     * events in changelog, where a rename is overriding a destination
     * file. */
    bool mds_has_lu543;
    bool mds_has_lu1331;

    /* file to dump all changelog records */
    char dump_file[RBH_PATH_MAX];

} chglog_reader_config_t;

/** start ChangeLog Readers
 * \param mdt_index -1 for all
 */
int cl_reader_start(run_flags_t flags, int mdt_index);

/** terminate ChangeLog Readers */
int cl_reader_terminate(void);

/** wait for ChangeLog Readers termination */
int cl_reader_wait(void);

/** Release last changelog records, and dump the final stats. */
int cl_reader_done(void);

/** dump changelog processing stats */
int cl_reader_dump_stats(void);

/** store changelog stats to db */
int cl_reader_store_stats(lmgr_t *lmgr);

/** config handlers */
extern mod_cfg_funcs_t cl_reader_cfg_hdlr;

/**
 * \addtogroup CHANGE_LOGS_UT
 * @{
 */

/* reader thread info, one per MDT */
typedef struct reader_thr_info_t
{
    /** reader thread index */
    unsigned int thr_index;

    /** thread id */
    pthread_t thr_id;

    /** open information */
    char *mdtdevice;
    int flags;

    /** nbr of records read by this thread */
    unsigned long long nb_read;

    /** number of records of interest (ie. not MARK, IOCTL, ...) */
    unsigned long long interesting_records;

    /** number of suppressed/merged records */
    unsigned long long suppressed_records;

    /** time when the last line was read */
    time_t  last_read_time;

    /** time of the last read record */
    struct timeval last_read_record_time;

    /** last read record id */
    unsigned long long last_read_record;

    /** last record id committed to database */
    unsigned long long last_committed_record;

    /** last record id cleared with changelog */
    unsigned long long last_cleared_record;

    /** last record pushed to the pipeline */
    unsigned long long last_pushed;

    /* number of times the changelog has been reopened */
    unsigned int nb_reopen;

    /** thread was asked to stop */
    unsigned int force_stop : 1;

    /** log handler */
    void * chglog_hdlr;

    /** Queue of pending changelogs to push to the pipeline. */
    struct rh_list_head op_queue;
    unsigned int op_queue_count;
    bool op_queue_updated;

    /** Store the ops for easier access. Each element in the hash
     * table is also in the op_queue list. This hash table doesn't
     * need a lock per slot since there is only one reader. The
     * slot counts won't be used either. */
    struct id_hash * id_hash;

    unsigned long long cl_counters[CL_LAST]; /* since program start time */
    /* last reported stat (for incremental diff) */
    unsigned long long cl_reported[CL_LAST];
    time_t last_report;

    /* to compute relative changelog speed (timeframe of read changelog since
     * the last report)
     **/
    struct timeval last_report_record_time;
    unsigned long long last_report_record_id;
    unsigned int last_reopen;

    /** On pre LU-1331 versions of Lustre, a CL_RENAME is always
     * followed by a CL_EXT, however these may not be
     * contiguous. Temporarily store the CL_RENAME changelog until we
     * get the CL_EXT. */
    CL_REC_TYPE * cl_rename;

} reader_thr_info_t;

/**
 * This handles a single log record.
 */
int process_log_rec( reader_thr_info_t * p_info, CL_REC_TYPE * p_rec );

bool unlink_compact(struct rh_list_head *op_queue,
                    unsigned int *op_queue_count);

void compact_op_queue(reader_thr_info_t *info);
/** @} */

#endif

/** @} */
