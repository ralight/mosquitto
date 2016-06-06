#ifndef MOSQUITTO_PERSIST_H
#define MOSQUITTO_PERSIST_H

#include <mosquitto_plugin.h>

#define MOSQ_PERSIST_PLUGIN_VERSION 1

struct mosquitto_plugin_opt{
	char *key;
	char *value;
};


int mosquitto_persist_plugin_version(int broker_version);

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



/* Implement the below in your plugin */
int mosquitto_persist_plugin_version(int broker_version);
int mosquitto_persist_plugin_init(void **userdata, struct mosquitto_plugin_opt *opts, int opt_count);
int mosquitto_persist_plugin_cleanup(void *userdata, struct mosquitto_plugin_opt *opts, int opt_count);
int mosquitto_persist_msg_store_restore(void *userdata);
int mosquitto_persist_msg_store_add(void *userdata, uint64_t dbid, const char *source_id, int source_mid, int mid, const char *topic, int qos, int retained, int payloadlen, const void *payload);
int mosquitto_persist_msg_store_delete(void *userdata, uint64_t dbid);
int mosquitto_persist_retain_add(void *userdata, uint64_t store_id);
int mosquitto_persist_retain_delete(void *userdata, uint64_t store_id);
int mosquitto_persist_retain_restore(void *userdata);
int mosquitto_persist_client_add(void *userdata, const char *client_id, int last_mid, time_t disconnect_t);
int mosquitto_persist_client_delete(void *userdata, const char *client_id);
int mosquitto_persist_client_restore(void *userdata);
int mosquitto_persist_subscription_add(void *userdata, const char *client_id, const char *topic, int qos);
int mosquitto_persist_subscription_delete(void *userdata, const char *client_id, const char *topic);
int mosquitto_persist_subscription_restore(void *userdata);
int mosquitto_persist_client_msg_add(void *userdata, const char *client_id, uint64_t store_id, int mid, int qos, bool retained, int direction, int state, bool dup);
int mosquitto_persist_client_msg_delete(void *userdata, const char *client_id, int mid, int direction);
int mosquitto_persist_client_msg_update(void *userdata, const char *client_id, int mid, int direction, int state, bool dup);
int mosquitto_persist_client_msg_restore(void *userdata);
int mosquitto_persist_transaction_begin(void *userdata);
int mosquitto_persist_transaction_end(void *userdata);

#endif
