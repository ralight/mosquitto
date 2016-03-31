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

	db->persist_plugin = mosquitto__calloc(1, sizeof(struct mosquitto__persist_plugin));
	if(!db->persist_plugin){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return 1;
	}

	db->persist_plugin->lib = LIB_LOAD(db->config->persistence_plugin);
	if(!db->persist_plugin->lib){
		log__printf(NULL, MOSQ_LOG_ERR, "Unable to load %s.", db->config->persistence_plugin);
		LIB_ERROR();
		db->persist_plugin->lib = NULL;
		mosquitto__free(db->persist_plugin);
		return 1;
	}

	plugin_version = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_plugin_version");
	if(!plugin_version){
		sym_error(&db->persist_plugin->lib, "plugin_version");
		mosquitto__free(db->persist_plugin);
		return 1;
	}

	version = plugin_version();
	if(version != MOSQ_PERSIST_PLUGIN_VERSION){
		log__printf(NULL, MOSQ_LOG_ERR,
				"Error: Incorrect persistence plugin version (got %d, expected %d).",
				version, MOSQ_PERSIST_PLUGIN_VERSION);

		LIB_CLOSE(db->persist_plugin->lib);
		db->persist_plugin->lib = NULL;
		mosquitto__free(db->persist_plugin);
		return 1;
	}

	db->persist_plugin->plugin_init = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_plugin_init");
	if(!db->persist_plugin->plugin_init){
		sym_error(&db->persist_plugin->lib, "plugin_init");
		mosquitto__free(db->persist_plugin);
		return 1;
	}

	db->persist_plugin->plugin_cleanup = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_plugin_cleanup");
	if(!db->persist_plugin->plugin_cleanup){
		sym_error(&db->persist_plugin->lib, "plugin_cleanup");
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
		return 1;
	}

	db->persist_plugin->msg_store_add = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_msg_store_add");
	if(!db->persist_plugin->msg_store_add){
		sym_error(&db->persist_plugin->lib, "msg_store_add");
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
		return 1;
	}

	db->persist_plugin->msg_store_delete = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_msg_store_delete");
	if(!db->persist_plugin->msg_store_delete){
		sym_error(&db->persist_plugin->lib, "msg_store_delete");
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
		return 1;
	}

	db->persist_plugin->retain_add = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_retain_add");
	if(!db->persist_plugin->retain_add){
		sym_error(&db->persist_plugin->lib, "retain_add");
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
		return 1;
	}

	db->persist_plugin->retain_delete = LIB_SYM(db->persist_plugin->lib, "mosquitto_persist_retain_delete");
	if(!db->persist_plugin->retain_delete){
		sym_error(&db->persist_plugin->lib, "retain_delete");
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
		return 1;
	}


	/* Initialise plugin */
	rc = db->persist_plugin->plugin_init(&db->persist_plugin->userdata, NULL, 0); /* FIXME - options */
	if(rc){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Persistence plugin initialisation returned error code %d.", rc);
		LIB_CLOSE(db->persist_plugin->lib);
		db->persist_plugin->lib = NULL;
		mosquitto__free(db->persist_plugin);
		return 1;
	}

	return 0;
}

int persist__plugin_cleanup(struct mosquitto_db *db)
{
	int rc = 0;

	if(db->persist_plugin->lib){
		if(db->persist_plugin->plugin_cleanup){
			rc = db->persist_plugin->plugin_cleanup(db->persist_plugin->userdata, NULL, 0); /* FIXME - options */
			if(rc){
				log__printf(NULL, MOSQ_LOG_ERR, "Error: Persistence plugin cleanup returned error code %d.", rc);
			}
		}
		LIB_CLOSE(db->persist_plugin->lib);
		db->persist_plugin->lib = NULL;
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
	}

	return rc;
}


/* ==================================================
 * Message store
 * ================================================== */
int persist__msg_store_add(struct mosquitto_db *db, struct mosquitto_msg_store *msg)
{
	int rc;

	if(msg->persisted) return 0;

	if(db->persist_plugin && db->persist_plugin->msg_store_add){
		rc = db->persist_plugin->msg_store_add(
				db->persist_plugin->userdata,
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

	return 0;
}

int persist__msg_store_delete(struct mosquitto_db *db, struct mosquitto_msg_store *msg)
{
	int rc;

	if(!msg->persisted) return 0;

	if(db->persist_plugin && db->persist_plugin->msg_store_delete){
		rc = db->persist_plugin->msg_store_delete(
				db->persist_plugin->userdata, msg->db_id);
		if(!rc){
			msg->persisted = false;
		}
		return rc;
	}

	return 0;
}


/* ==================================================
 * Retained messages
 * ================================================== */

int persist__retain_add(struct mosquitto_db *db, uint64_t store_id)
{
	int rc;

	if(db->persist_plugin && db->persist_plugin->retain_add){
		rc = db->persist_plugin->retain_add(
				db->persist_plugin->userdata, store_id);
		return rc;
	}

	return 0;
}

int persist__retain_delete(struct mosquitto_db *db, uint64_t store_id)
{
	int rc;

	if(db->persist_plugin && db->persist_plugin->retain_delete){
		rc = db->persist_plugin->retain_delete(
				db->persist_plugin->userdata, store_id);
		return rc;
	}

	return 0;
}
