/*
Copyright (c) 2016 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include <sqlite3.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mosquitto_persist.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"

/* ==================================================
 * Initialisation / cleanup
 * ================================================== */

struct mosquitto_sqlite {
	sqlite3 *db;
	sqlite3_stmt *msg_store_insert_stmt;
	sqlite3_stmt *msg_store_delete_stmt;
	sqlite3_stmt *retain_insert_stmt;
	sqlite3_stmt *retain_delete_stmt;
};

int mosquitto_persist_plugin_version(void)
{
	return MOSQ_PERSIST_PLUGIN_VERSION;
}

static int create_tables(struct mosquitto_sqlite *ud)
{
	int rc;

	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS msg_store "
			"("
				"dbid INTEGER PRIMARY KEY,"
				"source_id TEXT,"
				"source_mid INTEGER,"
				"mid INTEGER,"
				"topic TEXT,"
				"qos INTEGER,"
				"retained INTEGER,"
				"payloadlen INTEGER,"
				"payload BLOB"
			");",
			NULL, NULL, NULL);
	if(rc){
		mosquitto_log_printf(MOSQ_LOG_ERR, "Error in mosquitto_persist_plugin_init for sqlite plugin."); /* FIXME - print sqlite error */
		mosquitto_log_printf(MOSQ_LOG_ERR, "%s", sqlite3_errstr(rc));
		sqlite3_close(ud->db);
		return 1;
	}

	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS retained_msgs "
			"("
				"store_id INTEGER PRIMARY KEY"
			");",
			NULL, NULL, NULL);
	if(rc){
		mosquitto_log_printf(MOSQ_LOG_ERR, "Error in mosquitto_persist_plugin_init for sqlite plugin."); /* FIXME - print sqlite error */
		mosquitto_log_printf(MOSQ_LOG_ERR, "%s", sqlite3_errstr(rc));
		sqlite3_close(ud->db);
		return 1;
	}

	return 0;
}

int mosquitto_persist_plugin_init(void **userdata, struct mosquitto_plugin_opt *opts, int opt_count)
{
	struct mosquitto_sqlite *ud;
	int rc;

	ud = calloc(1, sizeof(struct mosquitto_sqlite));
	if(!ud){
		return 1;
	}

	if(sqlite3_open_v2("mosquitto.sqlite3", &ud->db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL) != SQLITE_OK){
		/* FIXME - handle error - use options for file */
	}else{
		rc = create_tables(ud);
		if(rc){
			return 1;
		}
	}
	/* FIXME - Load existing */

	/* Message store */
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO msg_store VALUES(?,?,?,?,?,?,?,?,?)",
			-1, &ud->msg_store_insert_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"DELETE FROM msg_store WHERE dbid=?",
			-1, &ud->msg_store_delete_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO retained_msgs (store_id) VALUES(?)",
			-1, &ud->retain_insert_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"DELETE FROM retained_msgs WHERE store_id=?",
			-1, &ud->retain_delete_stmt, NULL);

	*userdata = ud;
	return 0;
}

int mosquitto_persist_plugin_cleanup(void *userdata, struct mosquitto_plugin_opt *opts, int opt_count)
{
	struct mosquitto_sqlite *ud;
	if(userdata){
		ud = (struct mosquitto_sqlite *)userdata;
		if(ud->db){
			sqlite3_close(ud->db);
		}
		free(userdata);
	}

	return 0;
}



int mosquitto_persist_msg_store_add(void *userdata, uint64_t dbid, const char *source_id, int source_mid, int mid, const char *topic, int qos, int retained, int payloadlen, const void *payload)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_int64(ud->msg_store_insert_stmt, 1, dbid) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_text(ud->msg_store_insert_stmt, 2,
				source_id, strlen(source_id), SQLITE_STATIC) != SQLITE_OK){

		goto cleanup;
	}
	if(source_mid){
		if(sqlite3_bind_int(ud->msg_store_insert_stmt, 3, source_mid) != SQLITE_OK){
			goto cleanup;
		}
	}else{
		if(sqlite3_bind_null(ud->msg_store_insert_stmt, 3) != SQLITE_OK){
			goto cleanup;
		}
	}
	if(sqlite3_bind_int(ud->msg_store_insert_stmt, 4, mid) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_text(ud->msg_store_insert_stmt, 5,
				topic, strlen(topic), SQLITE_STATIC) != SQLITE_OK){

		goto cleanup;
	}
	if(sqlite3_bind_int(ud->msg_store_insert_stmt, 6, qos) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->msg_store_insert_stmt, 7, retained) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->msg_store_insert_stmt, 8, payloadlen) != SQLITE_OK){
		goto cleanup;
	}
	if(payloadlen){
		if(sqlite3_bind_blob(ud->msg_store_insert_stmt, 9,
					payload, payloadlen, SQLITE_STATIC) != SQLITE_OK){
			goto cleanup;
		}
	}else{
		if(sqlite3_bind_null(ud->msg_store_insert_stmt, 9) != SQLITE_OK){
			goto cleanup;
		}
	}

	if(sqlite3_step(ud->msg_store_insert_stmt) == SQLITE_DONE){
		rc = 0;
	}

cleanup:
	if(rc){
		mosquitto_log_printf(MOSQ_LOG_ERR, "SQLite error: %s\n", sqlite3_errmsg(ud->db));
	}
	sqlite3_reset(ud->msg_store_insert_stmt);
	sqlite3_clear_bindings(ud->msg_store_insert_stmt);
	return rc;
}


int mosquitto_persist_msg_store_delete(void *userdata, uint64_t dbid)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_int64(ud->msg_store_delete_stmt, 1, dbid) == SQLITE_OK){
		if(sqlite3_step(ud->msg_store_delete_stmt) == SQLITE_DONE){
			rc = 0;
		}
	}

	if(rc){
		mosquitto_log_printf(MOSQ_LOG_ERR, "SQLite error: %s\n", sqlite3_errmsg(ud->db));
	}
	sqlite3_reset(ud->msg_store_delete_stmt);
	sqlite3_clear_bindings(ud->msg_store_delete_stmt);
	return rc;
}


/* ==================================================
 * Retained messages
 * ================================================== */
static int sqlite__persist_retain(sqlite3_stmt *stmt, uint64_t store_id)
{
	int rc = 1;

	if(sqlite3_bind_int64(stmt, 1, store_id) == SQLITE_OK){
		if(sqlite3_step(stmt) == SQLITE_DONE){
			rc = 0;
		}
	}
	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);

	return rc;
}


int mosquitto_persist_retain_add(void *userdata, uint64_t store_id)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;

	return sqlite__persist_retain(ud->retain_insert_stmt, store_id);
}


int mosquitto_persist_retain_delete(void *userdata, uint64_t store_id)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;

	return sqlite__persist_retain(ud->retain_delete_stmt, store_id);
}

