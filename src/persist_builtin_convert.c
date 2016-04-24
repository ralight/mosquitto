/*
Copyright (c) 2010-2015 Roger Light <roger@atchoo.org>

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

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "mosquitto_broker.h"
#include "memory_mosq.h"
#include "persist_builtin.h"
#include "persist_plugin.h"
#include "time_mosq.h"
#include "util_mosq.h"


#define MOSQ_DB_VERSION 3

/* DB read/write */
const unsigned char magic[15] = {0x00, 0xB5, 0x00, 'm','o','s','q','u','i','t','t','o',' ','d','b'};
#define DB_CHUNK_CFG 1
#define DB_CHUNK_MSG_STORE 2
#define DB_CHUNK_CLIENT_MSG 3
#define DB_CHUNK_RETAIN 4
#define DB_CHUNK_SUB 5
#define DB_CHUNK_CLIENT 6
/* End DB read/write */

#define read_e(f, b, c) if(fread(b, 1, c, f) != c){ goto error; }
#define write_e(f, b, c) if(fwrite(b, 1, c, f) != c){ goto error; }


static uint32_t db_version;


static int persist__client_chunk_restore(struct mosquitto_db *db, FILE *db_fptr)
{
	uint16_t i16temp, slen, last_mid;
	char *client_id = NULL;
	int rc = 0;
	time_t disconnect_t;

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	if(!slen){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Corrupt persistent database.");
		fclose(db_fptr);
		return 1;
	}
	client_id = mosquitto__malloc(slen+1);
	if(!client_id){
		fclose(db_fptr);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	read_e(db_fptr, client_id, slen);
	client_id[slen] = '\0';

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	last_mid = ntohs(i16temp);

	if(db_version == 2){
		disconnect_t = time(NULL);
	}else{
		read_e(db_fptr, &disconnect_t, sizeof(time_t));
	}

	persist__client_add(db, client_id, last_mid, disconnect_t);

	mosquitto__free(client_id);

	return rc;
error:
	log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", strerror(errno));
	fclose(db_fptr);
	mosquitto__free(client_id);
	return 1;
}

static int persist__client_msg_chunk_restore(struct mosquitto_db *db, FILE *db_fptr)
{
	dbid_t i64temp, store_id;
	uint16_t i16temp, slen, mid;
	uint8_t qos, retain, direction, state, dup;
	char *client_id = NULL;
	int rc = 0;
	char err[256];

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	if(!slen){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Corrupt persistent database.");
		fclose(db_fptr);
		return 1;
	}
	client_id = mosquitto__malloc(slen+1);
	if(!client_id){
		fclose(db_fptr);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	read_e(db_fptr, client_id, slen);
	client_id[slen] = '\0';

	read_e(db_fptr, &i64temp, sizeof(dbid_t));
	store_id = i64temp;

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	mid = ntohs(i16temp);

	read_e(db_fptr, &qos, sizeof(uint8_t));
	read_e(db_fptr, &retain, sizeof(uint8_t));
	read_e(db_fptr, &direction, sizeof(uint8_t));
	read_e(db_fptr, &state, sizeof(uint8_t));
	read_e(db_fptr, &dup, sizeof(uint8_t));

	persist__client_msg_add_by_id(db, client_id, store_id, mid, qos, retain, direction, state, dup);

	mosquitto__free(client_id);

	return rc;
error:
	strerror_r(errno, err, 256);
	log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", err);
	fclose(db_fptr);
	mosquitto__free(client_id);
	return 1;
}

static int persist__msg_store_chunk_restore(struct mosquitto_db *db, FILE *db_fptr)
{
	dbid_t i64temp;
	uint32_t i32temp;
	uint16_t i16temp, slen;
	char *source_id = NULL;
	char *topic = NULL;
	struct mosquitto_msg_store stored;
	char err[256];

	memset(&stored, 0, sizeof(struct mosquitto_msg_store));

	read_e(db_fptr, &i64temp, sizeof(dbid_t));
	stored.db_id = i64temp;

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	if(slen){
		source_id = mosquitto__malloc(slen+1);
		if(!source_id){
			fclose(db_fptr);
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		read_e(db_fptr, source_id, slen);
		source_id[slen] = '\0';
		stored.source_id = source_id;
	}
	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	stored.source_mid = ntohs(i16temp);

	/* This is the mid - don't need it */
	read_e(db_fptr, &i16temp, sizeof(uint16_t));

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	if(slen){
		topic = mosquitto__malloc(slen+1);
		if(!topic){
			fclose(db_fptr);
			mosquitto__free(source_id);
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		read_e(db_fptr, topic, slen);
		topic[slen] = '\0';
		stored.topic = topic;
	}else{
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Invalid msg_store chunk when restoring persistent database.");
		fclose(db_fptr);
		mosquitto__free(source_id);
		return 1;
	}
	read_e(db_fptr, &stored.qos, sizeof(uint8_t));
	read_e(db_fptr, &stored.retain, sizeof(uint8_t));
	
	read_e(db_fptr, &i32temp, sizeof(uint32_t));
	stored.payloadlen = ntohl(i32temp);

	if(stored.payloadlen){
		if(UHPA_ALLOC(stored.payload, stored.payloadlen) == 0){
			fclose(db_fptr);
			mosquitto__free(source_id);
			mosquitto__free(topic);
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		read_e(db_fptr, UHPA_ACCESS(stored.payload, stored.payloadlen), stored.payloadlen);
	}

	persist__msg_store_add(db, &stored);
	return 0;
error:
	strerror_r(errno, err, 256);
	log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", err);
	fclose(db_fptr);
	mosquitto__free(source_id);
	mosquitto__free(topic);
	return 1;
}

static int persist__retain_chunk_restore(struct mosquitto_db *db, FILE *db_fptr)
{
	dbid_t i64temp;
	char err[256];

	if(fread(&i64temp, sizeof(dbid_t), 1, db_fptr) != 1){
		strerror_r(errno, err, 256);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", err);
		fclose(db_fptr);
		return 1;
	}
	persist__retain_add(db, i64temp);
	return MOSQ_ERR_SUCCESS;
}

static int persist__sub_chunk_restore(struct mosquitto_db *db, FILE *db_fptr)
{
	uint16_t i16temp, slen;
	uint8_t qos;
	char *client_id;
	char *topic;
	int rc = 0;
	char err[256];

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	client_id = mosquitto__malloc(slen+1);
	if(!client_id){
		fclose(db_fptr);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	read_e(db_fptr, client_id, slen);
	client_id[slen] = '\0';

	read_e(db_fptr, &i16temp, sizeof(uint16_t));
	slen = ntohs(i16temp);
	topic = mosquitto__malloc(slen+1);
	if(!topic){
		fclose(db_fptr);
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		mosquitto__free(client_id);
		return MOSQ_ERR_NOMEM;
	}
	read_e(db_fptr, topic, slen);
	topic[slen] = '\0';

	read_e(db_fptr, &qos, sizeof(uint8_t));

	if(persist__sub_add(db, client_id, topic, qos)){
	}

	mosquitto__free(client_id);
	mosquitto__free(topic);

	return rc;
error:
	strerror_r(errno, err, 256);
	log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", err);
	fclose(db_fptr);
	return 1;
}

int persist__builtin_restore(struct mosquitto_db *db)
{
	FILE *fptr;
	char header[15];
	int rc = 0;
	uint32_t crc;
	dbid_t i64temp;
	uint32_t i32temp, length;
	uint16_t i16temp, chunk;
	uint8_t i8temp;
	ssize_t rlen;
	char err[256];
	struct mosquitto_msg_store_load *load, *load_tmp;

	assert(db);
	assert(db->config);
	assert(db->config->persistence_filepath);

	db->msg_store_load = NULL;

	fptr = mosquitto__fopen(db->config->persistence_filepath, "rb");
	if(fptr == NULL) return MOSQ_ERR_SUCCESS;
	read_e(fptr, &header, 15);
	if(!memcmp(header, magic, 15)){
		// Restore DB as normal
		read_e(fptr, &crc, sizeof(uint32_t));
		read_e(fptr, &i32temp, sizeof(uint32_t));
		db_version = ntohl(i32temp);
		/* IMPORTANT - this is where compatibility checks are made.
		 * Is your DB change still compatible with previous versions?
		 */
		if(db_version > MOSQ_DB_VERSION && db_version != 0){
			if(db_version == 2){
				/* Addition of disconnect_t to client chunk in v3. */
			}else{
				fclose(fptr);
				log__printf(NULL, MOSQ_LOG_ERR, "Error: Unsupported persistent database format version %d (need version %d).", db_version, MOSQ_DB_VERSION);
				return 1;
			}
		}

		while(rlen = fread(&i16temp, sizeof(uint16_t), 1, fptr), rlen == 1){
			chunk = ntohs(i16temp);
			read_e(fptr, &i32temp, sizeof(uint32_t));
			length = ntohl(i32temp);
			switch(chunk){
				case DB_CHUNK_CFG:
					read_e(fptr, &i8temp, sizeof(uint8_t)); // shutdown
					read_e(fptr, &i8temp, sizeof(uint8_t)); // sizeof(dbid_t)
					if(i8temp != sizeof(dbid_t)){
						log__printf(NULL, MOSQ_LOG_ERR, "Error: Incompatible database configuration (dbid size is %d bytes, expected %lu)",
								i8temp, (unsigned long)sizeof(dbid_t));
						fclose(fptr);
						return 1;
					}
					read_e(fptr, &i64temp, sizeof(dbid_t));
					db->last_db_id = i64temp;
					break;

				case DB_CHUNK_MSG_STORE:
					if(persist__msg_store_chunk_restore(db, fptr)){
						return 1;
					}
					break;

				case DB_CHUNK_CLIENT_MSG:
					if(persist__client_msg_chunk_restore(db, fptr)){
						return 1;
					}
					break;

				case DB_CHUNK_RETAIN:
					if(persist__retain_chunk_restore(db, fptr)){
						return 1;
					}
					break;

				case DB_CHUNK_SUB:
					if(persist__sub_chunk_restore(db, fptr)){
						return 1;
					}
					break;

				case DB_CHUNK_CLIENT:
					if(persist__client_chunk_restore(db, fptr)){
						return 1;
					}
					break;

				default:
					log__printf(NULL, MOSQ_LOG_WARNING, "Warning: Unsupported chunk \"%d\" in persistent database file. Ignoring.", chunk);
					rlen = fread(&i16temp, sizeof(uint16_t), 1, fptr);
					return 0;
					break;
			}
		}
		if(rlen < 0) goto error;
	}else{
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to restore persistent database. Unrecognised file format.");
		rc = 1;
	}

	fclose(fptr);

	HASH_ITER(hh, db->msg_store_load, load, load_tmp){
		HASH_DELETE(hh, db->msg_store_load, load);
		mosquitto__free(load);
	}
	return rc;
error:
	strerror_r(errno, err, 256);
	log__printf(NULL, MOSQ_LOG_ERR, "Error: %s.", err);
	if(fptr) fclose(fptr);
	return 1;
}

int persist__builtin_backup(struct mosquitto_db *db, bool shutdown)
{
	return 0;
}
