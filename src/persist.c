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

#include "config.h"

#ifdef WITH_PERSISTENCE

#include "mosquitto_broker.h"
#include "persist_builtin.h"

int persist__backup(struct mosquitto_db *db, bool shutdown)
{
	if(db->config->persistence_plugin){
		return MOSQ_ERR_SUCCESS; /* FIXME - plugin backup */
	}else{
		return persist__builtin_backup(db, shutdown);
	}
}

int persist__restore(struct mosquitto_db *db)
{
	if(db->config->persistence_plugin){
		return MOSQ_ERR_SUCCESS; /* FIXME - plugin restore */
	}else{
		return persist__builtin_restore(db);
	}
}

#endif
