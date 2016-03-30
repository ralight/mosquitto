#ifndef MOSQUITTO_PERSIST_H
#define MOSQUITTO_PERSIST_H

#include <mosquitto_plugin.h>

#define MOSQ_PERSIST_PLUGIN_VERSION 1

struct mosquitto_plugin_opt{
	char *key;
	char *value;
};


int mosquitto_persist_plugin_version(void);

int mosquitto_persist_plugin_init(void **userdata, struct mosquitto_plugin_opt *opts, int opt_count);
int mosquitto_persist_plugin_cleanup(void *userdata, struct mosquitto_plugin_opt *opts, int opt_count);

#endif
