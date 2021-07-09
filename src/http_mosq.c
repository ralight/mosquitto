/*
Copyright (c) 2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR EDL-1.0

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include "config.h"

#include <assert.h>
#include <errno.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "memory_mosq.h"
#include "mqtt_protocol.h"
#include "net_mosq.h"
#include "packet_mosq.h"
#include "read_handle.h"
#include "sys_tree.h"
#include "util_mosq.h"

#if WITH_WEBSOCKETS == WS_IS_WSLAY
#include "picohttpparser.h"

int http__context_init(struct mosquitto *context)
{
	context->transport = mosq_t_http;
	context->http_request = mosquitto_calloc(1, db.config->websockets_headers_size);
	if(context->http_request == NULL){
		return MOSQ_ERR_NOMEM;
	}

	return MOSQ_ERR_SUCCESS;
}


int http__context_cleanup(struct mosquitto *context)
{
	mosquitto__free(context->http_request);
	context->http_request = NULL;
	return MOSQ_ERR_SUCCESS;
}


static int create_accept_key(const char *client_key, size_t client_key_len, char **encoded)
{
	const EVP_MD *digest;
	EVP_MD_CTX *evp;
	uint8_t accept_key_hash[EVP_MAX_MD_SIZE];
	unsigned int accept_key_hash_len;

	digest = EVP_get_digestbyname("sha1");
	if(!digest){
		return MOSQ_ERR_UNKNOWN;
	}

	evp = EVP_MD_CTX_new();
	if(EVP_DigestInit_ex(evp, digest, NULL) == 0){
		EVP_MD_CTX_free(evp);
		return MOSQ_ERR_UNKNOWN;
	}
	if(EVP_DigestUpdate(evp, client_key, client_key_len) == 0){
		EVP_MD_CTX_free(evp);
		return MOSQ_ERR_UNKNOWN;
	}
	if(EVP_DigestUpdate(evp, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
				strlen("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")) == 0){

		EVP_MD_CTX_free(evp);
		return MOSQ_ERR_UNKNOWN;
	}
	if(EVP_DigestFinal_ex(evp, accept_key_hash, &accept_key_hash_len) == 0){
		EVP_MD_CTX_free(evp);
		return MOSQ_ERR_UNKNOWN;
	}
	EVP_MD_CTX_free(evp);

	return base64__encode(accept_key_hash, accept_key_hash_len, encoded);
}


int http__write(struct mosquitto *mosq)
{
	return packet__write(mosq);
}


int http__read(struct mosquitto *mosq)
{
	ssize_t read_length;
	enum mosquitto_client_state state;
	size_t hlen;
	const char *http_method, *http_path;
	size_t http_method_len, http_path_len;
	int http_minor_version;
	size_t http_header_count = 100;
	struct phr_header http_headers[100];
	const char *client_key = NULL;
	size_t client_key_len = 0;
	char *accept_key;
	size_t i;
	bool header_have_upgrade;
	bool header_have_connection;
	struct mosquitto__packet *packet;
	int rc;

	if(!mosq){
		return MOSQ_ERR_INVAL;
	}
	if(mosq->sock == INVALID_SOCKET){
		return MOSQ_ERR_NO_CONN;
	}

	state = mosquitto__get_state(mosq);
	if(state == mosq_cs_connect_pending){
		return MOSQ_ERR_SUCCESS;
	}

	hlen = strlen(mosq->http_request);
	read_length = net__read(mosq, &mosq->http_request[hlen], db.config->websockets_headers_size-hlen);
	if(read_length <= 0){
		if(read_length == 0){
			return MOSQ_ERR_CONN_LOST; /* EOF */
		}
#ifdef WIN32
		errno = WSAGetLastError();
#endif
		if(errno == EAGAIN || errno == COMPAT_EWOULDBLOCK){
			return MOSQ_ERR_SUCCESS;
		}else{
			switch(errno){
				case COMPAT_ECONNRESET:
					return MOSQ_ERR_CONN_LOST;
				case COMPAT_EINTR:
					return MOSQ_ERR_SUCCESS;
				default:
					return MOSQ_ERR_ERRNO;
			}
		}
	}

	read_length = phr_parse_request(mosq->http_request, strlen(mosq->http_request),
			&http_method, &http_method_len,
			&http_path, &http_path_len,
			&http_minor_version,
			http_headers, &http_header_count,
			0);

	if(strncmp(http_method, "GET", http_method_len) && strncmp(http_method, "HEAD", http_method_len)){
		mosquitto__free(mosq->http_request);
		mosq->http_request = NULL;
		/* FIXME Not supported - send 501 response */
		return MOSQ_ERR_UNKNOWN;
	}

	header_have_upgrade = false;
	header_have_connection = false;

	for(i=0; i<http_header_count; i++){
		if(!strncasecmp(http_headers[i].name, "Upgrade", http_headers[i].name_len)){
			/* Check for "websocket" */
			if(!strncasecmp(http_headers[i].value, "websocket", http_headers[i].value_len)){
				header_have_upgrade = true;
			}
		}else if(!strncasecmp(http_headers[i].name, "Connection", http_headers[i].name_len)){
			/* Check for "upgrade" */
			if(!strncasecmp(http_headers[i].value, "upgrade", http_headers[i].value_len)){
				header_have_connection = true;
			}
		}else if(!strncasecmp(http_headers[i].name, "Sec-WebSocket-Key", http_headers[i].name_len)){
			client_key = http_headers[i].value;
			client_key_len = http_headers[i].value_len;
		}else if(!strncasecmp(http_headers[i].name, "Sec-WebSocket-Version", http_headers[i].name_len)){
			/* Check for "13" */
			if(http_headers[i].value_len != 2
					|| http_headers[i].value[0] != '1'
					|| http_headers[i].value[1] != '3'
					){

				/* FIXME - not supported */
				return MOSQ_ERR_NOT_SUPPORTED;
			}
		}else if(!strncasecmp(http_headers[i].name, "Sec-WebSocket-Protocol", http_headers[i].name_len)){
			/* Check for "mqtt" */
			if(strncmp(http_headers[i].value, "mqtt", http_headers[i].value_len)){
				/* FIXME - not supported */
				return MOSQ_ERR_NOT_SUPPORTED;
			}
		}else{
			/* Unknown header */
		}
	}

	if(header_have_upgrade == false || header_have_connection == false
			|| client_key == NULL || client_key_len == 0){

		// FIXME - 404
		return MOSQ_ERR_UNKNOWN;
	}

	if(create_accept_key(client_key, client_key_len, &accept_key)){
		return MOSQ_ERR_UNKNOWN;
	}

	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet) + 1024);
	if(!packet) return MOSQ_ERR_NOMEM;
	packet->packet_length = (uint32_t )snprintf((char *)packet->payload, 1024,
			"HTTP/1.1 101 Switching Protocols\r\n"
			"Upgrade: WebSocket\r\n"
			"Connection: Upgrade\r\n"
			"Sec-WebSocket-Accept: %s\r\n"
			"Sec-WebSocket-Protocol: mqtt\r\n"
			"\r\n", accept_key);
	free(accept_key);
	packet->to_process = packet->packet_length;

	mosq->http_request[0] = '\0';
	rc = packet__queue(mosq, packet);
	http__context_cleanup(mosq);
	ws__context_init(mosq);
	return rc;
}
#endif
