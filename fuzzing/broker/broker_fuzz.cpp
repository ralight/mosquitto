/*
Copyright (c) 2009-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include "broker_fuzz.h"

/* The broker fuzz-only main function. */
extern "C" int mosquitto_fuzz_main(int argc, char *argv[]);

void *run_broker(void *args)
{
	struct fuzz_data *fuzz = (struct fuzz_data *)args;
	char *argv[4];
	int argc = 4;
	char buf[20];

	argv[0] = strdup("mosquitto");
	argv[1] = strdup("-q");
	argv[2] = strdup("-p");
	snprintf(buf, sizeof(buf), "%d", fuzz->port);
	argv[3] = buf;

	mosquitto_fuzz_main(argc, argv);

	for(int i=0; i<3; i++){
		free(argv[i]);
	}

	pthread_exit(NULL);
	return NULL;
}


void recv_timeout(int sock, void *buf, size_t len, int timeout_us)
{
	struct timeval tv = {0, timeout_us};

	setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
	(void)recv(sock, buf, len, 0);
}

int connect_retrying(int port)
{
	struct sockaddr_in addr;
	int sock;
	int rc;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	sock = socket(AF_INET, SOCK_STREAM, 0);
	for(int i=0; i<500; i++){ /* 500x10ms = 5 seconds max wait */
		errno = 0;
		rc = connect(sock, (struct sockaddr *)&addr, sizeof(addr));
		if(rc < 0 && errno == ECONNREFUSED){
			struct timespec ts;
			ts.tv_sec = 0;
			ts.tv_nsec = 10000000; /* 10ms */
			nanosleep(&ts, NULL);
		}else if(rc < 0){
			return -1;
		}else{
			break;
		}
	}
	return sock;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
	struct fuzz_data fuzz;
	pthread_t thread;

	if(size < kMinInputLength || size > kMaxInputLength){
		return 0;
	}

	signal(SIGPIPE, SIG_IGN);

	memset(&fuzz, 0, sizeof(fuzz));
	fuzz.port = 1883;
	fuzz.size = size;
	fuzz.data = (uint8_t *)data;

	pthread_create(&thread, NULL, run_broker, &fuzz);
	run_client(&fuzz);
	pthread_join(thread, NULL);

	return 0;
}
