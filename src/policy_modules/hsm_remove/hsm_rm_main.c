/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define HSMRM_TAG "HSM_rm"

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "hsm_rm.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>


/* ----- queue management ----- */

static entry_queue_t hsm_rm_queue;

/* request status */
#define HSMRM_OK            0
#define HSMRM_ERROR         1

#define HSMRM_STATUS_COUNT  2

/* ----- Module configuration ----- */

hsm_rm_config_t hsm_rm_config;
static int     hsm_rm_flags = 0;

#define one_shot_mode (hsm_rm_flags & FLAG_ONCE)
#define dry_run (hsm_rm_flags & FLAG_DRY_RUN)

/* main thread */
static pthread_t hsm_rm_thr_id;

/* array of threads */
static pthread_t *thread_ids = NULL;

static time_t  last_rm = 0;

#define CHECK_QUEUE_INTERVAL    1

/* ---- Internal functions ---- */

/**
 * HSM rm helper
 * @return posix error code (from errno)
 */

static inline int HSM_rm( const entry_id_t * p_id )
{
    DisplayLog( LVL_FULL, HSMRM_TAG, "HSM_remove("DFID")", PFID(p_id) );
    if ( !dry_run )
          return LustreHSM_Action( HUA_REMOVE, p_id, NULL, 0 );
    return 0;
}

typedef struct hsm_rm_item__
{
    entry_id_t     entry_id;
} hsm_rm_item_t;

/**
 *  Alloc a new rm item so it can be pushes to the rm queue.
 */
static void   *MkRmItem( entry_id_t * p_entry_id )
{
    hsm_rm_item_t  *new_entry;

    new_entry = ( hsm_rm_item_t * ) MemAlloc( sizeof( hsm_rm_item_t ) );
    if ( !new_entry )
        return NULL;

    new_entry->entry_id = *p_entry_id;

    return new_entry;
}

/**
 * Free a Rmdir Item (and the resources of entry_attr).
 */
static void FreeRmItem( hsm_rm_item_t * item )
{
    MemFree( item );
}

/**
 *  Sum the number of acks from a status tab
 */
static inline unsigned int ack_count( unsigned int *status_tab )
{
    unsigned int   i, sum;
    sum = 0;

    for ( i = 0; i < HSMRM_STATUS_COUNT; i++ )
        sum += status_tab[i];

    return sum;
}

/**
 * This function retrieve files to be removed from HSM
 * and submit them to workers.
 */
static int perform_hsm_rm( unsigned int *p_nb_removed )
{
    int            rc;
    lmgr_t         lmgr;
    struct lmgr_rm_list_t *it = NULL;

    entry_id_t     entry_id;

    unsigned int   status_tab1[HSMRM_STATUS_COUNT];
    unsigned int   status_tab2[HSMRM_STATUS_COUNT];

    unsigned int   submitted_files, nb_in_queue, nb_hsm_rm_pending;

    int            end_of_list = FALSE;

    if ( p_nb_removed )
        *p_nb_removed = 0;

    /* we assume that purging is a rare event
     * and we don't want to use a DB connection all the time
     * so we connect only at purge time */

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Could not connect to database (error %d). Removal cancelled.", rc );
          return rc;
      }

    DisplayLog( LVL_EVENT, HSMRM_TAG, "Start removing files in HSM" );

    it = ListMgr_RmList( &lmgr, TRUE );

    if ( it == NULL )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Error retrieving list of removed entries from database. Operation cancelled." );
          return -1;
      }

    /* retrieve info before removing dirs, so we can make a delta after */
    RetrieveQueueStats( &hsm_rm_queue, NULL, NULL, NULL, NULL, NULL, status_tab1, NULL );

    submitted_files = 0;

    /* submit all eligible files (@TODO up to max_rm) */
    do
      {
          memset( &entry_id, 0, sizeof( entry_id_t ) );

          /* @TODO retrieve path and dates for traces */
          rc = ListMgr_GetNextRmEntry( it, &entry_id, NULL, NULL, NULL );

          if ( rc == DB_END_OF_LIST )
            {
                end_of_list = TRUE;
                break;
            }
          else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d getting next entry of iterator", rc );
                break;
            }

          /* submit entry for removal */
          rc = Queue_Insert( &hsm_rm_queue, MkRmItem( &entry_id ) );
          if ( rc )
              return rc;

          submitted_files++;
      }
    while ( 1 );                /* until DB_END_OF_LIST or error is returned */

    /* close iterator and db access */
    ListMgr_CloseRmList( it );
    ListMgr_CloseAccess( &lmgr );

    /* wait for end of rm pass  */
    do
      {
          RetrieveQueueStats( &hsm_rm_queue, NULL, &nb_in_queue, NULL, NULL, NULL, status_tab2,
                              NULL );

          /* nb of rm operation pending = nb_enqueued - ( nb ack after - nb ack before ) */
          nb_hsm_rm_pending = submitted_files + ack_count( status_tab1 ) - ack_count( status_tab2 );

          DisplayLog( LVL_DEBUG, HSMRM_TAG,
                      "Waiting for remove request queue: still %u files to be removed "
                      "(%u in queue, %u beeing processed)",
                      nb_hsm_rm_pending, nb_in_queue, nb_hsm_rm_pending - nb_in_queue );

          if ( ( nb_in_queue != 0 ) || ( nb_hsm_rm_pending != 0 ) )
              rh_sleep( CHECK_QUEUE_INTERVAL );

      }
    while ( ( nb_in_queue != 0 ) || ( nb_hsm_rm_pending != 0 ) );

    if ( p_nb_removed )
        *p_nb_removed = status_tab2[HSMRM_OK] - status_tab1[HSMRM_OK];

    if ( end_of_list )
        return 0;
    else
        return rc;
}

/**
 *  Worker thread that performs HSM_REMOVE
 */
static void   *Thr_Rm( void *arg )
{
    int            rc;
    lmgr_t         lmgr;

    void          *p_queue_entry;
    hsm_rm_item_t  *p_item;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Could not connect to database (error %d). Exiting.",
                      rc );
          exit( rc );
      }

    while ( Queue_Get( &hsm_rm_queue, &p_queue_entry ) == 0 )
    {
          p_item = ( hsm_rm_item_t * ) p_queue_entry;

          DisplayLog( LVL_FULL, HSMRM_TAG, "Considering entry "DFID,
                      PFID( &p_item->entry_id) );

          /* 7) rm and remove entry from database */
          rc = HSM_rm( &p_item->entry_id );

          if ( rc )
            {
                DisplayLog( LVL_DEBUG, HSMRM_TAG, "Error removing entry "DFID": %s",
                            PFID( &p_item->entry_id), strerror( abs(rc) ) );

                Queue_Acknowledge( &hsm_rm_queue, HSMRM_ERROR, NULL, 0 );

                /* free entry resources */
                FreeRmItem( p_item );

                continue;
            }
          else
            {
                /*char           strmod[256];*/

                /* request sucessfully sent */

                /* report messages */

                DisplayLog( LVL_DEBUG, HSMRM_TAG,
                            "Remove request successful for entry "DFID, PFID(&p_item->entry_id) );
/*                            "Remove request for entry "DFID", removed from Lustre %s ago", ... */

                DisplayReport( "HSM_remove "DFID, PFID(&p_item->entry_id) );
                           /*     " | lustre_rm=%" PRINT_TIME_T,
                               ATTR( &p_item->entry_attr, fullpath ), strmod, ATTR( &new_attr_set,
                                                                                    last_mod ) );   */
                /* remove it from database */
                rc = ListMgr_SoftRemove_Discard( &lmgr, &p_item->entry_id );
                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d removing entry from database.",
                                rc );

                /* ack to queue manager */
                Queue_Acknowledge( &hsm_rm_queue, HSMRM_OK, NULL, 0 );

                /* free entry resources */
                FreeRmItem( p_item );

            }
    }                         /* end of infinite loop en Queue_Get */

    return NULL;
}


int start_hsm_rm_threads( unsigned int nb_threads )
{
    unsigned int   i;

    thread_ids = ( pthread_t * ) MemCalloc( nb_threads, sizeof( pthread_t ) );
    if ( !thread_ids )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Memory error in %s", __FUNCTION__ );
          return ENOMEM;
      }

    for ( i = 0; i < nb_threads; i++ )
      {
          if ( pthread_create( &thread_ids[i], NULL, Thr_Rm, NULL ) != 0 )
            {
                int            rc = errno;
                DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d creating HSM_rm thread in %s: %s", rc,
                            __FUNCTION__, strerror( rc ) );
                return rc;
            }
      }

    return 0;
}

/**
 * Main loop for removal
 */
static void   *remove_thr( void *thr_arg )
{
    int            rc;
    unsigned int   nb_removed;

    do
      {
          if ( policies.unlink_policy.hsm_remove )
            {
                rc = perform_hsm_rm( &nb_removed );

                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG,
                                "perform_hsm_rm() returned with error %d. %u remove requests sent.",
                                rc, nb_removed );
                else
                    DisplayLog( LVL_MAJOR, HSMRM_TAG,
                                "HSM file removal summary: %u requests sent.", nb_removed );
            }
          else
            {
                DisplayLog( LVL_EVENT, HSMRM_TAG,
                            "HSM entry removal is disabled (hsm_remove_policy::hsm_remove = off)." );
            }

          last_rm = time( NULL );

          if ( !one_shot_mode )
              rh_sleep( hsm_rm_config.runtime_interval );
          else
            {
                pthread_exit( NULL );
                return NULL;
            }

      }
    while ( 1 );

    return NULL;

}

/* ------------ Exported functions ------------ */

/**
 * Initialize module and start main thread
 */
int Start_HSMRm( hsm_rm_config_t * p_config, int flags )
{
    int            rc;
    dev_t       fsdev;

    /* store configuration */
    hsm_rm_config = *p_config;
    hsm_rm_flags = flags;

    /* Check mount point and FS type.  */
    rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, &fsdev );
    if ( rc != 0 )
        return rc;

    if ( !policies.unlink_policy.hsm_remove )
    {
            DisplayLog( LVL_CRIT, HSMRM_TAG,
                "HSM removal is disabled in configuration file. Skipping module initialization..." );
            return ENOENT;
    }

    /* initialize rm queue */
    rc = CreateQueue( &hsm_rm_queue, hsm_rm_config.rm_queue_size, HSMRM_STATUS_COUNT - 1, 0 );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d initializing rm queue", rc );
          return rc;
      }
    else
         DisplayLog( LVL_FULL, HSMRM_TAG, "HSM rm queue created (size=%u)", hsm_rm_config.rm_queue_size );

    /* start rm threads */
    rc = start_hsm_rm_threads( hsm_rm_config.nb_threads_rm );
    if ( rc )
        return rc;

    /* start main thread */

    rc = pthread_create( &hsm_rm_thr_id, NULL, remove_thr, NULL );

    if ( rc != 0 )
      {
          rc = errno;
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Error %d starting main thread for HSM removal: %s", rc,
                      strerror( rc ) );
          return rc;
      }

    return 0;
}


int Wait_HSMRm(  )
{
    void          *returned;
    pthread_join( hsm_rm_thr_id, &returned );
    return 0;
}


void Dump_HSMRm_Stats(  )
{
    char           tmp_buff[256];
    struct tm      paramtm;

    unsigned int   status_tab[HSMRM_STATUS_COUNT];

    unsigned int   nb_waiting, nb_items;
    time_t         last_submitted, last_started, last_ack;
    time_t         now = time( NULL );

    DisplayLog( LVL_MAJOR, "STATS", "====== HSM Remove Stats ======" );
    if ( last_rm )
      {
          strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &last_rm, &paramtm ) );
          DisplayLog( LVL_MAJOR, "STATS", "last_run              = %s", tmp_buff );
      }
    else
        DisplayLog( LVL_MAJOR, "STATS", "last_run              = (none)" );

    /* Rmdir stats */

    RetrieveQueueStats( &hsm_rm_queue, &nb_waiting, &nb_items, &last_submitted, &last_started,
                        &last_ack, status_tab, NULL );

    DisplayLog( LVL_MAJOR, "STATS", "idle rm threads       = %u", nb_waiting );
    DisplayLog( LVL_MAJOR, "STATS", "rm requests pending   = %u", nb_items );
    DisplayLog( LVL_MAJOR, "STATS", "requests sent         = %u", status_tab[HSMRM_OK] );
    DisplayLog( LVL_MAJOR, "STATS", "errorneous requests   = %u", status_tab[HSMRM_ERROR] );

    if ( last_submitted )
        DisplayLog( LVL_MAJOR, "STATS", "last entry submitted %2d s ago",
                    ( int ) ( now - last_submitted ) );

    if ( last_started )
        DisplayLog( LVL_MAJOR, "STATS", "last entry handled   %2d s ago",
                    ( int ) ( now - last_started ) );

    if ( last_ack )
        DisplayLog( LVL_MAJOR, "STATS", "last request sent    %2d s ago",
                    ( int ) ( now - last_ack ) );


}
