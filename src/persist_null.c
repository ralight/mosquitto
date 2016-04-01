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
#include <stdlib.h>
#include <string.h>
#include "mosquitto_persist.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"

int persist__plugin_version_null(void)
{
	return MOSQ_PERSIST_PLUGIN_VERSION;
}

int persist__plugin_init_null(void **userdata, struct mosquitto_plugin_opt *opts, int opt_count)
{
	return 0;
}

int persist__plugin_cleanup_null(void *userdata, struct mosquitto_plugin_opt *opts, int opt_count)
{
	return 0;
}

int persist__msg_store_add_null(void *userdata, uint64_t dbid, const char *source_id, int source_mid, int mid, const char *topic, int qos, int retained, int payloadlen, const void *payload)
{
	return 0;
}

int persist__msg_store_delete_null(void *userdata, uint64_t dbid)
{
	return 0;
}

int persist__msg_store_restore_null(void *userdata)
{
	return 0;
}

int persist__retain_add_null(void *userdata, uint64_t store_id)
{
	return 0;
}


int persist__retain_delete_null(void *userdata, uint64_t store_id)
{
	return 0;
}

int persist__retain_restore_null(void *userdata)
{
	return 0;
}

