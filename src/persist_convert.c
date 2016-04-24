#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include "mosquitto_broker.h"
#include "persist_builtin.h"
#include "persist_plugin.h"
#include "mosquitto_persist.h"


bool flag_reload = false;
#ifdef WITH_PERSISTENCE
bool flag_db_backup = false;
#endif
bool flag_tree_print = false;
int run;

struct mosquitto_db int_db;

int drop_privileges(struct mosquitto__config *config, bool temporary)
{
	return 0;
}

int restore_privileges(void)
{
	return 0;
}

struct mosquitto_db *mosquitto__get_db(void)
{
	return &int_db;
}

int persist_convert(char *dbfile, char *sqlfile)
{
	int rc;
	struct mosquitto__config config;

	memset(&config, 0, sizeof(struct mosquitto__config));
	memset(&int_db, 0, sizeof(struct mosquitto_db));

	int_db.config = &config;
	config.log_dest = MQTT3_LOG_STDOUT;
	config.log_type = MOSQ_LOG_ALL;
	log__init(&config);

	config.persistence = true;
	config.persistence_plugin = "mosquitto_persist_sqlite.so";
	config.persistence_filepath = dbfile;

	rc = persist__plugin_init(&int_db);
	config.persistence_plugin = NULL;
	rc = db__open(&config, &int_db);


	return 0;
}


int main(int argc, char *argv[])
{
	if(argc != 3){
		printf("Usage: persist_convert <mosquitto.db> <mosquitto.sqlite3>\n");
		return 1;
	}

	return persist_convert(argv[1], argv[2]);
}

