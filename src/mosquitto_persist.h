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


int mosquitto_persist_msg_store_load(
		uint64_t dbid, const char *source_id, int source_mid,
		int mid, const char *topic, int qos, int retained,
		int payloadlen, const void *payload);

int mosquitto_persist_retain_load(uint64_t store_id);

int mosquitto_persist_client_load(const char *client_id, int last_mid, time_t disconnect_t);

int mosquitto_persist_subscription_load(const char *client_id, const char *topic, int qos);

int mosquitto_persist_client_msg_load(const char *client_id, uint64_t store_id, int mid, int qos, int retained, int direction, int state, int dup);
#endif
