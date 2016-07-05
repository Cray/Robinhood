/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 *  vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2016 Seagate Technology LLC
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#include <mysql.h>
#include <errno.h>
#include <stdlib.h>

int STDCALL mysql_real_query(MYSQL *mysql, const char *q, unsigned long length)
{
    (void)mysql;
    (void)q;
    (void)length;
    return ENOSYS;
}

unsigned int STDCALL mysql_errno(MYSQL *mysql)
{
    (void)mysql;
    return ENOSYS;
}

const char * STDCALL mysql_error(MYSQL *mysql)
{
    (void)mysql;
    return NULL;
}

MYSQL_RES * STDCALL mysql_store_result(MYSQL *mysql)
{
    (void)mysql;
    return NULL;
}

MYSQL * STDCALL mysql_init(MYSQL *mysql)
{
    (void)mysql;
    return NULL;
}

int STDCALL mysql_options(MYSQL *mysql, enum mysql_option option,
              const void *arg)
{
    (void)mysql;
    (void)option;
    (void)arg;
    return ENOSYS;
}

MYSQL * STDCALL mysql_real_connect(MYSQL *mysql, const char *host,
                   const char *user, const char *passwd,
                   const char *db, unsigned int port,
                   const char *unix_socket,
                   unsigned long clientflag)
{
    (void)mysql;
    (void)host;
    (void)user;
    (void)passwd;
    (void)db;
    (void)port;
    (void)unix_socket;
    (void)clientflag;
    return NULL;
}

void STDCALL mysql_close(MYSQL *sock)
{
    (void)sock;
}

void STDCALL mysql_free_result(MYSQL_RES *result)
{
    (void)result;
}

MYSQL_ROW STDCALL mysql_fetch_row(MYSQL_RES *result)
{
    (void)result;
    return NULL;
}

unsigned int STDCALL mysql_num_fields(MYSQL_RES *res)
{
    (void)res;
    return 0;
}

my_ulonglong STDCALL mysql_num_rows(MYSQL_RES *res)
{
    (void)res;
    return 0;
}

unsigned long STDCALL mysql_real_escape_string(MYSQL *mysql, char *to,
                           const char *from,
                           unsigned long length)
{
    (void)mysql;
    (void)to;
    (void)from;
    (void)length;
    return 0;
}

unsigned long STDCALL mysql_get_server_version(MYSQL *mysql)
{
    (void)mysql;
    return 0;
}

my_ulonglong STDCALL mysql_insert_id(MYSQL *mysql)
{
    (void)mysql;
    return 0;
}
