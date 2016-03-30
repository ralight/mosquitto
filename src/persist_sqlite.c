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
};

int mosquitto_persist_plugin_version(void)
{
	return MOSQ_PERSIST_PLUGIN_VERSION;
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
		rc = sqlite3_exec(ud->db,
				"CREATE TABLE msg_store "
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
			sqlite3_close(ud->db);
			return 1;
		}
	}

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

