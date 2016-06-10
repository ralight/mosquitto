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
#include <stdio.h>
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
	sqlite3_stmt *client_insert_stmt;
	sqlite3_stmt *client_delete_stmt;
	sqlite3_stmt *sub_insert_stmt;
	sqlite3_stmt *sub_delete_stmt;
	sqlite3_stmt *sub_select_stmt;
	sqlite3_stmt *sub_update_stmt;
	sqlite3_stmt *client_msg_insert_stmt;
	sqlite3_stmt *client_msg_delete_stmt;
	sqlite3_stmt *client_msg_update_stmt;
	sqlite3_stmt *transaction_begin_stmt;
	sqlite3_stmt *transaction_end_stmt;
	int synchronous;
};

int mosquitto_persist_plugin_version(int broker_version)
{
	if(broker_version <= MOSQ_PERSIST_PLUGIN_VERSION){
		return MOSQ_PERSIST_PLUGIN_VERSION;
	}else{
		return -1;
	}
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
				"topic TEXT NOT NULL,"
				"qos INTEGER,"
				"retained INTEGER,"
				"payloadlen INTEGER,"
				"payload BLOB"
			");",
			NULL, NULL, NULL);
	if(rc) goto error;


	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS retained_msgs "
			"("
				"store_id INTEGER PRIMARY KEY,"
				"FOREIGN KEY(store_id) REFERENCES msg_store(dbid) "
				"ON DELETE CASCADE"
			");",
			NULL, NULL, NULL);
	if(rc) goto error;


	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS clients "
			"("
				"client_id TEXT PRIMARY KEY,"
				"last_mid INTEGER,"
				"disconnect_t INTEGER"
			");",
			NULL, NULL, NULL);
	if(rc) goto error;


	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS subscriptions "
			"("
				"client_id TEXT NOT NULL,"
				"topic TEXT NOT NULL,"
				"qos INTEGER,"
				"FOREIGN KEY(client_id) REFERENCES clients(client_id) "
				"ON DELETE CASCADE"
			");",
			NULL, NULL, NULL);
	if(rc) goto error;

	rc = sqlite3_exec(ud->db,
			"CREATE TABLE IF NOT EXISTS client_msgs "
			"("
				"client_id TEXT NOT NULL,"
				"store_id INTEGER,"
				"mid INTEGER,"
				"qos INTEGER,"
				"retained INTEGER,"
				"direction INTEGER,"
				"state INTEGER,"
				"dup INTEGER,"
				"FOREIGN KEY(store_id) REFERENCES msg_store(dbid)"
				"ON DELETE CASCADE,"
				"FOREIGN KEY(client_id) REFERENCES clients(client_id)"
				"ON DELETE CASCADE"
			");",
			NULL, NULL, NULL);
	if(rc) goto error;

	return 0;

error:
	mosquitto_log_printf(MOSQ_LOG_ERR, "Error in mosquitto_persist_plugin_init for sqlite plugin."); /* FIXME - print sqlite error */
	mosquitto_log_printf(MOSQ_LOG_ERR, "%s", sqlite3_errstr(rc));
	sqlite3_close(ud->db);
	return 1;
}


static int prepare_statements(struct mosquitto_sqlite *ud)
{
	int rc;

	/* Message store */
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO msg_store "
			"(dbid,source_id,source_mid,mid,topic,qos,retained,payloadlen,payload) "
			"VALUES(?,?,?,?,?,?,?,?,?)",
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
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO clients (client_id, last_mid, disconnect_t) VALUES(?,?,?)",
			-1, &ud->client_insert_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"DELETE FROM clients WHERE client_id=?",
			-1, &ud->client_delete_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO subscriptions (client_id, topic, qos) VALUES(?,?,?)",
			-1, &ud->sub_insert_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"DELETE FROM subscriptions WHERE client_id=? AND topic=?",
			-1, &ud->sub_delete_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"SELECT * FROM subscriptions WHERE client_id=? AND topic=?",
			-1, &ud->sub_select_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"UPDATE subscriptions set qos=? WHERE client_id=? AND topic=?",
			-1, &ud->sub_update_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"INSERT INTO client_msgs "
			"(client_id, store_id, mid, qos, retained, direction, state, dup) "
			"VALUES(?,?,?,?,?,?,?,?)",
			-1, &ud->client_msg_insert_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"DELETE FROM client_msgs WHERE client_id=? AND mid=? AND direction=?",
			-1, &ud->client_msg_delete_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db,
			"UPDATE client_msgs set state=? WHERE client_id=? AND mid=? AND direction=?",
			-1, &ud->client_msg_update_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db, "BEGIN TRANSACTION",
			-1, &ud->transaction_begin_stmt, NULL);
	rc = sqlite3_prepare_v2(ud->db, "END TRANSACTION",
			-1, &ud->transaction_end_stmt, NULL);

	return rc;
}

int mosquitto_persist_plugin_init(void **userdata, struct mosquitto_plugin_opt *opts, int opt_count)
{
	struct mosquitto_sqlite *ud;
	int rc;
	int i;
	char buf[100];

	ud = calloc(1, sizeof(struct mosquitto_sqlite));
	if(!ud){
		return 1;
	}
	ud->synchronous = 1;
	
	char *path = NULL; // ral? should these go inside "mosquitto_sqlite" or not? (probably? need to be freeed?)
	char *file = "mosquitto.sqlite3";

	for(i=0; i<opt_count; i++){
		if(!strcmp(opts[i].key, "persist_opt_sync")){
			if(!strcmp(opts[i].value, "extra")){
				ud->synchronous = 3;
			}else if(!strcmp(opts[i].value, "full")){
				ud->synchronous = 2;
			}else if(!strcmp(opts[i].value, "normal")){
				ud->synchronous = 1;
			}else if(!strcmp(opts[i].value, "off")){
				ud->synchronous = 0;
			}else{
				// FIXME unknown option
				mosquitto_log_printf(MOSQ_LOG_ERR, "Error: Invalid persist_opt_sync value '%s'.", opts[i].value);
				return 1;
			}
		}
		if (!strcmp(opts[i].key, "persist_location")) {
			if(strlen(opts[i].value)) {
				int len = strlen(opts[i].value) + strlen(file) + 1;
//				path = mosquitto__malloc(len);
				path = malloc(len);
				if (!path) return MOSQ_ERR_NOMEM;  // not sure this is in your plugin api sorry!
				snprintf(path, len, "%s%s", opts[i].value, file);
			}
		}
	}
	if (!path) {
		path = file;
	}

	mosquitto_log_printf(MOSQ_LOG_NOTICE, "operating with db: %s", path);
	if(sqlite3_open_v2(path, &ud->db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL) != SQLITE_OK){
		/* FIXME - handle error - use options for file */
	}else{
		rc = sqlite3_exec(ud->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
		rc = sqlite3_exec(ud->db, "PRAGMA page_size=32768;", NULL, NULL, NULL);
		snprintf(buf, 100, "PRAGMA synchronous=%d;", ud->synchronous);
		rc = sqlite3_exec(ud->db, buf, NULL, NULL, NULL);

		rc = create_tables(ud);
		if(rc){
			return 1;
		}
	}

	prepare_statements(ud);

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


/* ==================================================
 * Message store
 * ================================================== */

int mosquitto_persist_msg_store_restore(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt;
	int rc = 1;

	uint64_t dbid;
	const char *source_id;
	int source_mid;
	int mid;
	const char *topic;
	int qos;
	int retained;
	int payloadlen;
	const void *payload;

	rc = sqlite3_prepare_v2(ud->db,
			"SELECT dbid,source_id,source_mid,mid,topic,qos,retained,payloadlen,payload FROM msg_store",
			-1, &stmt, NULL);
	while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		dbid = sqlite3_column_int64(stmt, 0);
		source_id = (const char *)sqlite3_column_text(stmt, 1);
		source_mid = sqlite3_column_int(stmt, 2);
		mid = sqlite3_column_int(stmt, 3);
		topic = (const char *)sqlite3_column_text(stmt, 4);
		qos = sqlite3_column_int(stmt, 5);
		retained = sqlite3_column_int(stmt, 6);
		payloadlen = sqlite3_column_int(stmt, 7);
		payload = sqlite3_column_blob(stmt, 8);

		mosquitto_persist_msg_store_load(
				dbid, source_id, source_mid,
				mid, topic, qos, retained,
				payloadlen, payload);
	}
	/* FIXME - check rc */
	sqlite3_finalize(stmt);

	return 0;
}


int mosquitto_persist_msg_store_add(void *userdata, uint64_t dbid, const char *source_id, int source_mid, int mid, const char *topic, int qos, int retained, int payloadlen, const void *payload)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_int64(ud->msg_store_insert_stmt, 1, dbid) != SQLITE_OK){
		goto cleanup;
	}
	if(source_id){
		if(sqlite3_bind_text(ud->msg_store_insert_stmt, 2,
					source_id, strlen(source_id), SQLITE_STATIC) != SQLITE_OK){

			goto cleanup;
		}
	}else{
		if(sqlite3_bind_null(ud->msg_store_insert_stmt, 2) != SQLITE_OK){
			goto cleanup;
		}
	}
	if(sqlite3_bind_int(ud->msg_store_insert_stmt, 3, source_mid) != SQLITE_OK){
		goto cleanup;
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
		mosquitto_log_printf(MOSQ_LOG_ERR, "SQLite error: %s", sqlite3_errmsg(ud->db));
	}
	sqlite3_reset(ud->msg_store_insert_stmt);
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
		mosquitto_log_printf(MOSQ_LOG_ERR, "SQLite error: %s", sqlite3_errmsg(ud->db));
	}
	sqlite3_reset(ud->msg_store_delete_stmt);
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


int mosquitto_persist_retain_restore(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt;
	int rc = 1;
	int rc2;

	uint64_t dbid;

	rc = sqlite3_prepare_v2(ud->db,
			"SELECT * FROM retained_msgs",
			-1, &stmt, NULL);
	while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		dbid = sqlite3_column_int64(stmt, 0);

		rc2 = mosquitto_persist_retain_load(dbid);
		if(rc2){
			return rc2;
		}
	}
	/* FIXME - check rc */
	sqlite3_finalize(stmt);

	return 0;
}


/* ==================================================
 * Clients
 * ================================================== */

int mosquitto_persist_client_add(void *userdata, const char *client_id, int last_mid, time_t disconnect_t)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;
	int rc2;

	if(sqlite3_bind_text(ud->client_insert_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_bind_int(ud->client_insert_stmt, 2, last_mid) == SQLITE_OK){
			if(sqlite3_bind_int64(ud->client_insert_stmt, 3, disconnect_t) == SQLITE_OK){
				rc2 = sqlite3_step(ud->client_insert_stmt);
				if(rc2 == SQLITE_DONE){
					rc = 0;
				}
			}
		}
	}
	sqlite3_reset(ud->client_insert_stmt);

	return rc;
}


int mosquitto_persist_client_delete(void *userdata, const char *client_id)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_text(ud->client_delete_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_step(ud->client_delete_stmt) == SQLITE_DONE){
			rc = 0;
		}
	}
	sqlite3_reset(ud->client_delete_stmt);

	return rc;
}


int mosquitto_persist_client_restore(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt;
	int rc = 1;

	const char *client_id;
	int last_mid;
	time_t disconnect_t;

	rc = sqlite3_prepare_v2(ud->db,
			"SELECT client_id,last_mid,disconnect_t FROM clients",
			-1, &stmt, NULL);
	while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		client_id = (const char *)sqlite3_column_text(stmt, 0);
		last_mid = sqlite3_column_int(stmt, 1);
		disconnect_t = sqlite3_column_int64(stmt, 2);

		mosquitto_persist_client_load(
				client_id, last_mid, disconnect_t);
	}
	/* FIXME - check rc */
	sqlite3_finalize(stmt);

	return 0;
}


/* ==================================================
 * Subscriptions
 * ================================================== */

int mosquitto_persist_subscription_add(void *userdata, const char *client_id, const char *topic, int qos)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt = NULL;
	int rc = 1;
	int rc2;
	int client_id_pos, topic_pos, qos_pos;

	if(sqlite3_bind_text(ud->sub_select_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_bind_text(ud->sub_select_stmt, 2, topic, strlen(topic), SQLITE_STATIC) == SQLITE_OK){
			rc2 = sqlite3_step(ud->sub_select_stmt);
			if(rc2 == SQLITE_ROW){
				stmt = ud->sub_update_stmt;
				qos_pos = 1;
				client_id_pos = 2;
				topic_pos = 3;
			}else if(rc2 == SQLITE_DONE){
				stmt = ud->sub_insert_stmt;
				client_id_pos = 1;
				topic_pos = 2;
				qos_pos = 3;
			}
		}
	}
	sqlite3_reset(ud->sub_select_stmt);

	if(!stmt){
		return 1;
	}

	if(sqlite3_bind_text(stmt, client_id_pos, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_bind_text(stmt, topic_pos, topic, strlen(topic), SQLITE_STATIC) == SQLITE_OK){
			if(sqlite3_bind_int(stmt, qos_pos, qos) == SQLITE_OK){
				rc2 = sqlite3_step(stmt);
				if(rc2 == SQLITE_DONE){
					rc = 0;
				}
			}
		}
	}
	sqlite3_reset(stmt);

	return rc;
}


int mosquitto_persist_subscription_delete(void *userdata, const char *client_id, const char *topic)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_text(ud->sub_delete_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_bind_text(ud->sub_delete_stmt, 2, topic, strlen(topic), SQLITE_STATIC) == SQLITE_OK){
			if(sqlite3_step(ud->sub_delete_stmt) == SQLITE_DONE){
				rc = 0;
			}
		}
	}
	sqlite3_reset(ud->sub_delete_stmt);

	return rc;
}


int mosquitto_persist_subscription_restore(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt;
	int rc = 1;
	int rc2;

	const char *client_id;
	const char *topic;
	int qos;

	rc = sqlite3_prepare_v2(ud->db,
			"SELECT client_id,topic,qos FROM subscriptions",
			-1, &stmt, NULL);
	while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		client_id = (const char *)sqlite3_column_text(stmt, 0);
		topic = (const char *)sqlite3_column_text(stmt, 1);
		qos = sqlite3_column_int(stmt, 2);

		rc2 = mosquitto_persist_subscription_load(
				client_id, topic, qos);
		if(rc2) return rc2;
	}
	sqlite3_finalize(stmt);

	return 0;
}


/* ==================================================
 * Client messages
 * ================================================== */

int mosquitto_persist_client_msg_add(void *userdata, const char *client_id, uint64_t store_id, int mid, int qos, bool retained, int direction, int state, bool dup)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_text(ud->client_msg_insert_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int64(ud->client_msg_insert_stmt, 2, store_id) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 3, mid) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 4, qos) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 5, retained) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 6, direction) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 7, state) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_bind_int(ud->client_msg_insert_stmt, 8, dup) != SQLITE_OK){
		goto cleanup;
	}
	if(sqlite3_step(ud->client_msg_insert_stmt) == SQLITE_DONE){
		rc = 0;
	}

cleanup:
	if(rc){
		mosquitto_log_printf(MOSQ_LOG_ERR, "SQLite error: %s", sqlite3_errmsg(ud->db));
	}
	sqlite3_reset(ud->client_msg_insert_stmt);
	return rc;
}


int mosquitto_persist_client_msg_delete(void *userdata, const char *client_id, int mid, int direction)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_text(ud->client_msg_delete_stmt, 1, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
		if(sqlite3_bind_int(ud->client_msg_delete_stmt, 2, mid) == SQLITE_OK){
			if(sqlite3_bind_int(ud->client_msg_delete_stmt, 3, direction) == SQLITE_OK){
				if(sqlite3_step(ud->client_msg_delete_stmt) == SQLITE_DONE){
					rc = 0;
				}
			}
		}
	}
	sqlite3_reset(ud->sub_delete_stmt);

	return rc;
}


int mosquitto_persist_client_msg_update(void *userdata, const char *client_id, int mid, int direction, int state, bool dup)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_bind_int(ud->client_msg_update_stmt, 1, state) == SQLITE_OK){
		if(sqlite3_bind_text(ud->client_msg_update_stmt, 2, client_id, strlen(client_id), SQLITE_STATIC) == SQLITE_OK){
			if(sqlite3_bind_int(ud->client_msg_update_stmt, 3, mid) == SQLITE_OK){
				if(sqlite3_bind_int(ud->client_msg_update_stmt, 4, direction) == SQLITE_OK){
					if(sqlite3_step(ud->client_msg_update_stmt) == SQLITE_DONE){
						rc = 0;
					}
				}
			}
		}
	}
	sqlite3_reset(ud->sub_update_stmt);

	return rc;
}


int mosquitto_persist_client_msg_restore(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	sqlite3_stmt *stmt;
	int rc = 1;
	int rc2;

	const char *client_id;
	uint64_t store_id;
	int mid;
	int qos;
	int retained;
	int direction;
	int state;
	int dup;

	rc = sqlite3_prepare_v2(ud->db,
			"SELECT client_id,store_id,mid,qos,retained,direction,state,dup "
			"FROM client_msgs",
			-1, &stmt, NULL);
	while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
		client_id = (const char *)sqlite3_column_text(stmt, 0);
		store_id = sqlite3_column_int64(stmt, 1);
		mid = sqlite3_column_int(stmt, 2);
		qos = sqlite3_column_int(stmt, 3);
		retained = sqlite3_column_int(stmt, 4);
		direction = sqlite3_column_int(stmt, 5);
		state = sqlite3_column_int(stmt, 6);
		dup = sqlite3_column_int(stmt, 7);

		rc2 = mosquitto_persist_client_msg_load(
				client_id, store_id, mid,
				qos, retained, direction,
				state, dup);
		if(rc2) return rc2;
	}
	sqlite3_finalize(stmt);

	return 0;
}


/* ==================================================
 * Transactions
 * ================================================== */

int mosquitto_persist_transaction_begin(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_step(ud->transaction_begin_stmt) == SQLITE_DONE){
		rc = 0;
	}
	sqlite3_reset(ud->transaction_begin_stmt);

	return rc;
}


int mosquitto_persist_transaction_end(void *userdata)
{
	struct mosquitto_sqlite *ud = (struct mosquitto_sqlite *)userdata;
	int rc = 1;

	if(sqlite3_step(ud->transaction_end_stmt) == SQLITE_DONE){
		rc = 0;
	}
	sqlite3_reset(ud->transaction_end_stmt);

	return rc;
}

