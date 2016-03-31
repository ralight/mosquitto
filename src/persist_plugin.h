#ifndef PERSIST_PLUGIN_H
#define PERSIST_PLUGIN_H

#include "mosquitto_broker.h"
#include <stdint.h>

int persist__plugin_init(struct mosquitto_db *db);
int persist__plugin_cleanup(struct mosquitto_db *db);

int persist__msg_store_add(struct mosquitto_db *db, struct mosquitto_msg_store *msg);
int persist__msg_store_delete(struct mosquitto_db *db, struct mosquitto_msg_store *msg);
#endif
