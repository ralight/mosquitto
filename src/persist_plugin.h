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

#ifndef PERSIST_PLUGIN_H
#define PERSIST_PLUGIN_H

#include "mosquitto_broker.h"
#include <stdint.h>

int persist__plugin_init(struct mosquitto_db *db);
int persist__plugin_cleanup(struct mosquitto_db *db);

int persist__msg_store_add(struct mosquitto_db *db, struct mosquitto_msg_store *msg);
int persist__msg_store_delete(struct mosquitto_db *db, struct mosquitto_msg_store *msg);

int persist__retain_add(struct mosquitto_db *db, uint64_t store_id);
int persist__retain_delete(struct mosquitto_db *db, uint64_t store_id);

#endif
