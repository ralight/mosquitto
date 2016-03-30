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
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Persistence plugin cleanup returned error code %d.", rc);
		}
		LIB_CLOSE(db->persist_plugin->lib);
		db->persist_plugin->lib = NULL;
		mosquitto__free(db->persist_plugin);
		db->persist_plugin = NULL;
	}

	return rc;
}
