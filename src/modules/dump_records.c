/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2017 Seagate Technology LLC
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file dump_records.c
 * \brief Implements Robinhood changelog post-processor dumping in-memory DB.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "status_manager.h"
#include "entry_processor.h"
#include "rbh_logs.h"
#include "rbh_misc.h"

/* for logs */
#define COLLAPSE_TAG  "dump_records"

bool           dump_records( struct rh_list_head * op_queue,
                             unsigned int *op_queue_count,
                             void *cpp_instance_data );

const char    *mod_get_name( void )
{
    return COLLAPSE_TAG;
}

status_manager_t *mod_get_status_manager( void )
{
    return NULL;
}

static chglog_postproc_t dump_records_cpp = {
    .name = COLLAPSE_TAG,
    .action = dump_records,
    .instance_data = NULL
};

chglog_postproc_t *mod_get_changelog_postproc( void )
{
    return &dump_records_cpp;
}

action_func_t mod_get_action_by_name( const char *action_name )
{
    return NULL;
}

#define LUSTRE_FIDS_EQUAL(_fid1, _fid2) \
    (  (_fid1).f_seq == (_fid2).f_seq   \
    && (_fid1).f_oid == (_fid2).f_oid   \
    && (_fid1).f_ver == (_fid2).f_ver)

#ifdef _LUSTRE_HSM
static const char *get_event_name( unsigned int cl_event )
{
    static const char *event_name[] = {
        "archive", "restore", "cancel", "release", "remove", "state",
    };

    if ( cl_event >= G_N_ELEMENTS( event_name ) )
        return "unknown";
    else
        return event_name[cl_event];
}
#endif

#define CL_BASE_FORMAT "%s: %llu %02d%-5s %u.%09u 0x%x%s t="DFID
#define CL_BASE_ARG(_mdt, _rec_) \
    (_mdt), (_rec_)->cr_index, \
    (_rec_)->cr_type, changelog_type2str((_rec_)->cr_type), \
    (uint32_t)cltime2sec((_rec_)->cr_time), cltime2nsec((_rec_)->cr_time), \
    (_rec_)->cr_flags & CLF_FLAGMASK, flag_buff, PFID(&(_rec_)->cr_tfid)
#define CL_NAME_FORMAT "p="DFID" %.*s"
#define CL_NAME_ARG(_rec_) PFID(&(_rec_)->cr_pfid), (_rec_)->cr_namelen, \
        rh_get_cl_cr_name(_rec_)

#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)
#define CL_EXT_FORMAT   "s="DFID" sp="DFID" %.*s"
#endif

/* Dump a single record. */
static void dump_record( int debug_level, const char *mdt,
                         const CL_REC_TYPE * rec )
{
    char           flag_buff[256] = "";
    char           record_str[RBH_PATH_MAX] = "";
    char          *curr = record_str;
    int            len;
    int            left = sizeof( record_str );

#ifdef _LUSTRE_HSM
    if ( rec->cr_type == CL_HSM )
        g_snprintf( flag_buff, sizeof( flag_buff ), "(%s%s,rc=%d)",
                    get_event_name( hsm_get_cl_event( rec->cr_flags ) ),
                    hsm_get_cl_flags( rec->
                                      cr_flags ) & CLF_HSM_DIRTY ? ",dirty" :
                    "", hsm_get_cl_error( rec->cr_flags ) );
#endif

    len = snprintf( curr, left, CL_BASE_FORMAT, CL_BASE_ARG( mdt, rec ) );
    curr += len;
    left -= len;
    if ( left > 0 && rec->cr_namelen )
    {
        /* this record has a 'name' field. */
        len = snprintf( curr, left, " " CL_NAME_FORMAT, CL_NAME_ARG( rec ) );
        curr += len;
        left -= len;
    }

    if ( left > 0 )
    {
#if defined(HAVE_FLEX_CL)
        /* Newer versions. The cr_sfid is not directly in the
         * changelog record anymore. CLF_RENAME is always present for
         * backward compatibility; it describes the format of the
         * record, but the rename extension will be zero'ed for
         * non-rename records...
         */
        if ( rec->cr_flags & CLF_RENAME )
        {
            struct changelog_ext_rename *cr_rename;

            cr_rename = changelog_rec_rename( ( CL_REC_TYPE * ) rec );
            if ( fid_is_sane( &cr_rename->cr_sfid ) )
            {
                len = snprintf( curr, left, " " CL_EXT_FORMAT,
                                PFID( &cr_rename->cr_sfid ),
                                PFID( &cr_rename->cr_spfid ),
                                ( int )
                                changelog_rec_snamelen( ( CL_REC_TYPE * ) rec ),
                                changelog_rec_sname( ( CL_REC_TYPE * ) rec ) );
                curr += len;
                left -= len;
            }
        }
        if ( rec->cr_flags & CLF_JOBID )
        {
            struct changelog_ext_jobid *jobid;
            jobid = changelog_rec_jobid( ( CL_REC_TYPE * ) rec );

            len = snprintf( curr, left, " J=%s", jobid->cr_jobid );
            curr += len;
            left -= len;
        }
#elif defined(HAVE_CHANGELOG_EXTEND_REC)
        if ( fid_is_sane( &rec->cr_sfid ) )
        {
            len = snprintf( curr, left, " " CL_EXT_FORMAT,
                            PFID( &rec->cr_sfid ),
                            PFID( &rec->cr_spfid ),
                            ( int ) changelog_rec_snamelen( rec ),
                            changelog_rec_sname( rec ) );
            curr += len;
            left -= len;
        }
#endif
    }

    if ( left <= 0 )
        record_str[RBH_PATH_MAX - 1] = '\0';

    DisplayLog( debug_level, COLLAPSE_TAG, "%s", record_str );
}

bool dump_records( struct rh_list_head *op_queue, unsigned int *op_queue_count,
               void *cpp_instance_data )
{
    entry_proc_op_t    *op;
    changelog_record_t *rec;

    /* Lookup starting from the last operation in the slot. */
    rh_list_for_each_entry( op, op_queue, list )
    {
        if ( op->extra_info_is_set == 0 ) /* No ChangeLog record in the
                                             operation */
            continue;

        rec = &op->extra_info.log_record;
        dump_record( LVL_FULL, rec->mdt, rec->p_log_rec );
    }

    return true;
}
