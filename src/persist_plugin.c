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

#include <stdint.h>

#include "lib_load.h"
#include "memory_mosq.h"
#include "mosquitto_broker.h"
#include "mosquitto_persist.h"
#include "persist_null.h"

#include <uthash.h>

static void sym_error(void **lib, const char *func)
{
	log__printf(NULL, MOSQ_LOG_ERR,
			"Error: Unable to load persistence plugin function mosquitto_persist_%s().", func);
	LIB_ERROR();
	LIB_CLOSE(*lib);
	*lib = NULL;
}

int persist__plugin_init(struct mosquitto_db *db)
{
	int (*plugin_version)(void);
	int version;
	int rc;

	db->persist_plugin.restoring = false;

	if(db->config->persistence_plugin == NULL){
		db->persist_plugin.lib = NULL;
		db->persist_plugin.userdata = NULL;
		db->persist_plugin.plugin_version = persist__plugin_version_null;
		db->persist_plugin.plugin_init = persist__plugin_init_null;
		db->persist_plugin.plugin_cleanup = persist__plugin_cleanup_null;
		db->persist_plugin.msg_store_add = persist__msg_store_add_null;
		db->persist_plugin.msg_store_delete = persist__msg_store_delete_null;
		db->persist_plugin.msg_store_restore = persist__msg_store_restore_null;
		db->persist_plugin.retain_add = persist__retain_add_null;
		db->persist_plugin.retain_delete = persist__retain_delete_null;
		db->persist_plugin.retain_restore = persist__retain_restore_null;
		db->persist_plugin.client_add = persist__client_add_null;
		db->persist_plugin.client_delete = persist__client_delete_null;
		db->persist_plugin.client_restore = persist__client_restore_null;
		db->persist_plugin.sub_add = persist__sub_add_null;
		db->persist_plugin.sub_delete = persist__sub_delete_null;
		db->persist_plugin.sub_restore = persist__sub_restore_null;
		db->persist_plugin.client_msg_add = persist__client_msg_add_null;
		db->persist_plugin.client_msg_delete = persist__client_msg_delete_null;
		db->persist_plugin.client_msg_update = persist__client_msg_update_null;
		db->persist_plugin.client_msg_restore = persist__client_msg_restore_null;
		db->persist_plugin.transaction_begin = persist__transaction_begin_null;
		db->persist_plugin.transaction_end = persist__transaction_end_null;
		return 0;
	}

	db->persist_plugin.lib = LIB_LOAD(db->config->persistence_plugin);
	if(!db->persist_plugin.lib){
		log__printf(NULL, MOSQ_LOG_ERR, "Unable to load %s.", db->config->persistence_plugin);
		LIB_ERROR();
		db->persist_plugin.lib = NULL;
		return 1;
	}

	plugin_version = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_plugin_version");
	if(!plugin_version){
		sym_error(&db->persist_plugin.lib, "plugin_version");
		return 1;
	}

	version = plugin_version();
	if(version != MOSQ_PERSIST_PLUGIN_VERSION){
		log__printf(NULL, MOSQ_LOG_ERR,
				"Error: Incorrect persistence plugin version (got %d, expected %d).",
				version, MOSQ_PERSIST_PLUGIN_VERSION);

		LIB_CLOSE(db->persist_plugin.lib);
		db->persist_plugin.lib = NULL;
		return 1;
	}

	db->persist_plugin.plugin_init = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_plugin_init");
	if(!db->persist_plugin.plugin_init){
		sym_error(&db->persist_plugin.lib, "plugin_init");
		return 1;
	}

	db->persist_plugin.plugin_cleanup = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_plugin_cleanup");
	if(!db->persist_plugin.plugin_cleanup){
		sym_error(&db->persist_plugin.lib, "plugin_cleanup");
		return 1;
	}

	db->persist_plugin.msg_store_add = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_msg_store_add");
	if(!db->persist_plugin.msg_store_add){
		sym_error(&db->persist_plugin.lib, "msg_store_add");
		return 1;
	}

	db->persist_plugin.msg_store_delete = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_msg_store_delete");
	if(!db->persist_plugin.msg_store_delete){
		sym_error(&db->persist_plugin.lib, "msg_store_delete");
		return 1;
	}

	db->persist_plugin.msg_store_restore = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_msg_store_restore");
	if(!db->persist_plugin.msg_store_restore){
		sym_error(&db->persist_plugin.lib, "msg_store_restore");
		return 1;
	}

	db->persist_plugin.retain_add = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_retain_add");
	if(!db->persist_plugin.retain_add){
		sym_error(&db->persist_plugin.lib, "retain_add");
		return 1;
	}

	db->persist_plugin.retain_delete = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_retain_delete");
	if(!db->persist_plugin.retain_delete){
		sym_error(&db->persist_plugin.lib, "retain_delete");
		return 1;
	}

	db->persist_plugin.retain_restore = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_retain_restore");
	if(!db->persist_plugin.retain_restore){
		sym_error(&db->persist_plugin.lib, "retain_restore");
		return 1;
	}

	db->persist_plugin.client_add = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_add");
	if(!db->persist_plugin.client_add){
		sym_error(&db->persist_plugin.lib, "client_add");
		return 1;
	}

	db->persist_plugin.client_delete = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_delete");
	if(!db->persist_plugin.client_delete){
		sym_error(&db->persist_plugin.lib, "client_delete");
		return 1;
	}

	db->persist_plugin.client_restore = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_restore");
	if(!db->persist_plugin.client_restore){
		sym_error(&db->persist_plugin.lib, "client_restore");
		return 1;
	}

	db->persist_plugin.sub_add = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_subscription_add");
	if(!db->persist_plugin.sub_add){
		sym_error(&db->persist_plugin.lib, "subscription_add");
		return 1;
	}

	db->persist_plugin.sub_delete = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_subscription_delete");
	if(!db->persist_plugin.sub_delete){
		sym_error(&db->persist_plugin.lib, "subscription_delete");
		return 1;
	}

	db->persist_plugin.sub_restore = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_subscription_restore");
	if(!db->persist_plugin.sub_restore){
		sym_error(&db->persist_plugin.lib, "subscription_restore");
		return 1;
	}

	db->persist_plugin.client_msg_add = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_msg_add");
	if(!db->persist_plugin.client_msg_add){
		sym_error(&db->persist_plugin.lib, "client_msg_add");
		return 1;
	}

	db->persist_plugin.client_msg_delete = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_msg_delete");
	if(!db->persist_plugin.client_msg_delete){
		sym_error(&db->persist_plugin.lib, "client_msg_delete");
		return 1;
	}

	db->persist_plugin.client_msg_update = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_msg_update");
	if(!db->persist_plugin.client_msg_update){
		sym_error(&db->persist_plugin.lib, "client_msg_update");
		return 1;
	}

	db->persist_plugin.client_msg_restore = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_client_msg_restore");
	if(!db->persist_plugin.client_msg_restore){
		sym_error(&db->persist_plugin.lib, "client_msg_restore");
		return 1;
	}

	db->persist_plugin.transaction_begin = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_transaction_begin");
	if(!db->persist_plugin.transaction_begin){
		sym_error(&db->persist_plugin.lib, "transaction_begin");
		return 1;
	}

	db->persist_plugin.transaction_end = LIB_SYM(db->persist_plugin.lib, "mosquitto_persist_transaction_end");
	if(!db->persist_plugin.transaction_end){
		sym_error(&db->persist_plugin.lib, "transaction_end");
		return 1;
	}


	/* Initialise plugin */
	rc = db->persist_plugin.plugin_init(&db->persist_plugin.userdata, NULL, 0); /* FIXME - options */
	if(rc){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Persistence plugin initialisation returned error code %d.", rc);
		LIB_CLOSE(db->persist_plugin.lib);
		db->persist_plugin.lib = NULL;
		return 1;
	}

	return 0;
}

int persist__plugin_cleanup(struct mosquitto_db *db)
{
	int rc = 0;

	if(db->persist_plugin.lib){
		if(db->persist_plugin.plugin_cleanup){
			rc = db->persist_plugin.plugin_cleanup(db->persist_plugin.userdata, NULL, 0); /* FIXME - options */
			if(rc){
				log__printf(NULL, MOSQ_LOG_ERR, "Error: Persistence plugin cleanup returned error code %d.", rc);
			}
		}
		LIB_CLOSE(db->persist_plugin.lib);
		db->persist_plugin.lib = NULL;
	}

	db->persist_plugin.userdata = NULL;
	db->persist_plugin.plugin_version = persist__plugin_version_null;
	db->persist_plugin.plugin_init = persist__plugin_init_null;
	db->persist_plugin.plugin_cleanup = persist__plugin_cleanup_null;
	db->persist_plugin.msg_store_add = persist__msg_store_add_null;
	db->persist_plugin.msg_store_delete = persist__msg_store_delete_null;
	db->persist_plugin.msg_store_restore = persist__msg_store_restore_null;
	db->persist_plugin.retain_add = persist__retain_add_null;
	db->persist_plugin.retain_delete = persist__retain_delete_null;
	db->persist_plugin.retain_restore = persist__retain_restore_null;
	db->persist_plugin.sub_add = persist__sub_add_null;
	db->persist_plugin.sub_delete = persist__sub_delete_null;
	db->persist_plugin.sub_restore = persist__sub_restore_null;
	db->persist_plugin.client_msg_add = persist__client_msg_add_null;
	db->persist_plugin.client_msg_delete = persist__client_msg_delete_null;
	db->persist_plugin.client_msg_update = persist__client_msg_update_null;
	db->persist_plugin.client_msg_restore = persist__client_msg_restore_null;
	db->persist_plugin.transaction_begin = persist__transaction_begin_null;
	db->persist_plugin.transaction_end = persist__transaction_end_null;

	return rc;
}


/* ==================================================
 * Restoring
 * ================================================== */
int persist__plugin_restore(struct mosquitto_db *db)
{
	int rc;

	db->persist_plugin.restoring = true;

	rc = db->persist_plugin.msg_store_restore(db->persist_plugin.userdata);
	if(rc){
		db->persist_plugin.restoring = false;
		return rc;
	}
	rc = db->persist_plugin.retain_restore(db->persist_plugin.userdata);
	if(rc){
		db->persist_plugin.restoring = false;
		return rc;
	}
	rc = db->persist_plugin.client_restore(db->persist_plugin.userdata);
	if(rc){
		db->persist_plugin.restoring = false;
		return rc;
	}
	rc = db->persist_plugin.sub_restore(db->persist_plugin.userdata);
	if(rc){
		db->persist_plugin.restoring = false;
		return rc;
	}
	rc = db->persist_plugin.client_msg_restore(db->persist_plugin.userdata);
	if(rc){
		db->persist_plugin.restoring = false;
		return rc;
	}

	db->persist_plugin.restoring = false;
	return 0;
}


int mosquitto_persist_msg_store_load(
		uint64_t dbid, const char *source_id, int source_mid,
		int mid, const char *topic, int qos, int retained,
		int payloadlen, const void *payload)
{
	struct mosquitto_msg_store *stored = NULL;
	struct mosquitto_msg_store_load *load;
	int rc;
	char *topic_local;
	mosquitto__payload_uhpa payload_local;

	struct mosquitto_db *db = mosquitto__get_db();

	if(!db->persist_plugin.restoring){
		return 1;
	}

	if(dbid > db->last_db_id){
		db->last_db_id = dbid;
	}

	load = mosquitto__malloc(sizeof(struct mosquitto_msg_store_load));
	if(!load){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}

	topic_local = mosquitto__strdup(topic);
	if(!topic_local){
		mosquitto__free(load);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}

	if(payloadlen){
		if(UHPA_ALLOC(payload_local, payloadlen) == 0){
			mosquitto__free(load);
			mosquitto__free(topic_local);
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		memcpy(UHPA_ACCESS(payload_local, payloadlen), payload, payloadlen);
	}
	rc = db__message_store(db, source_id, source_mid, topic_local, qos, payloadlen, &payload_local, retained, &stored, dbid);

	if(rc == MOSQ_ERR_SUCCESS){
		load->db_id = stored->db_id;
		load->store = stored;
		load->store->persisted = true;

		HASH_ADD(hh, db->msg_store_load, db_id, sizeof(uint64_t), load);
		return MOSQ_ERR_SUCCESS;
	}else{
		mosquitto__free(load);
		return rc;
	}
}


int mosquitto_persist_retain_load(uint64_t store_id)
{
	struct mosquitto_msg_store_load *load;
	struct mosquitto_db *db = mosquitto__get_db();

	if(!db->persist_plugin.restoring){
		return 1;
	}

	HASH_FIND(hh, db->msg_store_load, &store_id, sizeof(uint64_t), load);
	if(load){
		sub__messages_queue(db, NULL, load->store->topic, load->store->qos, load->store->retain, &load->store, false);
	}else{
		log__printf(NULL, MOSQ_LOG_ERR,
				"Error: Corrupt database whilst restoring a retained message.");
		return MOSQ_ERR_INVAL;
	}

	return MOSQ_ERR_SUCCESS;
}


int mosquitto_persist_client_load(const char *client_id, int last_mid, time_t disconnect_t)
{
	struct mosquitto *context, *found_context;
	struct mosquitto_db *db = mosquitto__get_db();

	if(!db->persist_plugin.restoring){
		return 1;
	}

	HASH_FIND(hh_id, db->contexts_by_id, client_id, strlen(client_id), found_context);
	if(found_context){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Duplicate client when restoring persistent database.");
		return 1;
	}

	context = context__init(db, -1);
	if(!context){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_INVAL;
	}

	context->id = mosquitto__strdup(client_id);
	if(!context->id){
		mosquitto__free(context);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_INVAL;
	}
	context->last_mid = last_mid;
	context->disconnect_t = disconnect_t;

	HASH_ADD_KEYPTR(hh_id, db->contexts_by_id, context->id, strlen(context->id), context);

	return MOSQ_ERR_SUCCESS;
}


int mosquitto_persist_subscription_load(const char *client_id, const char *topic, int qos)
{
	struct mosquitto *context;
	struct mosquitto_db *db = mosquitto__get_db();
	int rc;

	if(!db->persist_plugin.restoring){
		return 1;
	}

	HASH_FIND(hh_id, db->contexts_by_id, client_id, strlen(client_id), context);
	if(!context){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Missing client when restoring persistent database.");
		return 1;
	}

	rc = sub__add(db, context, topic, qos, &db->subs);
	if(rc > 0){
		return rc;
	}

	return MOSQ_ERR_SUCCESS;
}


int mosquitto_persist_client_msg_load(const char *client_id, uint64_t store_id, int mid, int qos, int retained, int direction, int state, int dup)
{
	struct mosquitto *context;
	struct mosquitto_msg_store_load *load;
	struct mosquitto_db *db = mosquitto__get_db();

	if(!db->persist_plugin.restoring){
		return 1;
	}

	HASH_FIND(hh_id, db->contexts_by_id, client_id, strlen(client_id), context);
	if(!context){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Missing client when restoring persistent database.");
		return 1;
	}

	HASH_FIND(hh, db->msg_store_load, &store_id, sizeof(uint64_t), load);
	if(!load){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Missing stored message when restoring persistent database.");
		return 1;
	}

	return db__message_insert(db, context, mid, direction, qos, retained, load->store);
}



/* ==================================================
 * Message store
 * ================================================== */
int persist__msg_store_add(struct mosquitto_db *db, struct mosquitto_msg_store *msg)
{
	int rc;

	if(msg->persisted) return 0;

	rc = db->persist_plugin.msg_store_add(
			db->persist_plugin.userdata,
			msg->db_id,
			msg->source_id,
			msg->source_mid,
			msg->mid,
			msg->topic,
			msg->qos,
			msg->retain,
			msg->payloadlen,
			UHPA_ACCESS_PAYLOAD(msg));
	if(!rc){
		msg->persisted = true;
	}
	return rc;
}

int persist__msg_store_delete(struct mosquitto_db *db, struct mosquitto_msg_store *msg)
{
	int rc;

	if(!msg->persisted) return 0;

	rc = db->persist_plugin.msg_store_delete(
			db->persist_plugin.userdata, msg->db_id);
	if(!rc){
		msg->persisted = false;
	}
	return rc;
}


/* ==================================================
 * Retained messages
 * ================================================== */

int persist__retain_add(struct mosquitto_db *db, uint64_t store_id)
{
	int rc;

	rc = db->persist_plugin.retain_add(
			db->persist_plugin.userdata, store_id);
	return rc;
}

int persist__retain_delete(struct mosquitto_db *db, uint64_t store_id)
{
	int rc;

	rc = db->persist_plugin.retain_delete(
			db->persist_plugin.userdata, store_id);
	return rc;
}


/* ==================================================
 * Client
 * ================================================== */

int persist__client_add(struct mosquitto_db *db, const char *client_id, int last_mid, time_t disconnect_t)
{
	int rc;

	rc = db->persist_plugin.client_add(
			db->persist_plugin.userdata, client_id, last_mid, disconnect_t);
	return rc;
}

int persist__client_delete(struct mosquitto_db *db, const char *client_id)
{
	int rc;

	rc = db->persist_plugin.client_delete(
			db->persist_plugin.userdata, client_id);
	return rc;
}


/* ==================================================
 * Subscription
 * ================================================== */

int persist__sub_add(struct mosquitto_db *db, const char *client_id, const char *topic, int qos)
{
	int rc;

	rc = db->persist_plugin.sub_add(
			db->persist_plugin.userdata, client_id, topic, qos);
	return rc;
}

int persist__sub_delete(struct mosquitto_db *db, const char *client_id, const char *topic)
{
	int rc;

	rc = db->persist_plugin.sub_delete(
			db->persist_plugin.userdata, client_id, topic);
	return rc;
}


/* ==================================================
 * Client messages
 * ================================================== */

int persist__client_msg_add(struct mosquitto_db *db, const char *client_id, struct mosquitto_msg_store *stored, int mid, int qos, int retained, int direction, int state, int dup)
{
	int rc;

	/* FIXME */ persist__msg_store_add(db, stored);
	rc = db->persist_plugin.client_msg_add(
			db->persist_plugin.userdata, client_id, stored->db_id, mid, qos, retained, direction, state, dup);
	return rc;
}


int persist__client_msg_delete(struct mosquitto_db *db, const char *client_id, int mid, int direction)
{
	int rc;

	rc = db->persist_plugin.client_msg_delete(
			db->persist_plugin.userdata, client_id, mid, direction);
	return rc;
}


int persist__client_msg_update(struct mosquitto_db *db, const char *client_id, int mid, int direction, int state, int dup)
{
	int rc;

	rc = db->persist_plugin.client_msg_update(
			db->persist_plugin.userdata, client_id, mid, direction, state, dup);
	return rc;
}


/* ==================================================
 * Transactions
 * ================================================== */

int persist__transaction_begin(struct mosquitto_db *db)
{
	return db->persist_plugin.transaction_begin(db->persist_plugin.userdata);
}

int persist__transaction_end(struct mosquitto_db *db)
{
	return db->persist_plugin.transaction_end(db->persist_plugin.userdata);
}

