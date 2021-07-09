/*
Copyright (c) 2009-2020 Roger Light <roger@atchoo.org>

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

#include "config.h"

#ifndef WIN32
/* For initgroups() */
#  include <unistd.h>
#  include <grp.h>
#  include <assert.h>
#endif

#ifndef WIN32
#include <pwd.h>
#else
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#ifndef WIN32
#  include <sys/time.h>
#endif

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#ifdef WITH_SYSTEMD
#  include <systemd/sd-daemon.h>
#endif
#ifdef WITH_WRAP
#include <tcpd.h>
#endif
#if WITH_WEBSOCKETS == WS_IS_LWS
#  include <libwebsockets.h>
#endif

#include "mosquitto_broker_internal.h"
#include "memory_mosq.h"
#include "misc_mosq.h"
#include "util_mosq.h"

struct mosquitto_db db;

struct mosquitto__listener_sock *g_listensock = NULL;
int g_listensock_count = 0;

bool flag_reload = false;
#ifdef WITH_PERSISTENCE
bool flag_db_backup = false;
#endif
bool flag_tree_print = false;
int g_run = 0;
#ifdef WITH_WRAP
#include <syslog.h>
int allow_severity = LOG_INFO;
int deny_severity = LOG_INFO;
#endif

/* mosquitto shouldn't run as root.
 * This function will attempt to change to an unprivileged user and group if
 * running as root. The user is given in config->user.
 * Returns 1 on failure (unknown user, setuid/setgid failure)
 * Returns 0 on success.
 * Note that setting config->user to "root" does not produce an error, but it
 * strongly discouraged.
 */
int drop_privileges(struct mosquitto__config *config)
{
#if !defined(__CYGWIN__) && !defined(WIN32)
	struct passwd *pwd;
	char *err;
	int rc;

	const char *snap = getenv("SNAP_NAME");
	if(snap && !strcmp(snap, "mosquitto")){
		/* Don't attempt to drop privileges if running as a snap */
		return MOSQ_ERR_SUCCESS;
	}

	if(geteuid() == 0){
		if(config->user && strcmp(config->user, "root")){
			pwd = getpwnam(config->user);
			if(!pwd){
				if(strcmp(config->user, "mosquitto")){
					log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to drop privileges to '%s' because this user does not exist.", config->user);
					return 1;
				}else{
					log__printf(NULL, MOSQ_LOG_ERR, "Warning: Unable to drop privileges to '%s' because this user does not exist. Trying 'nobody' instead.", config->user);
					pwd = getpwnam("nobody");
					if(!pwd){
						log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to drop privileges to 'nobody'.");
						return 1;
					}
				}
			}
			if(initgroups(config->user, pwd->pw_gid) == -1){
				err = strerror(errno);
				log__printf(NULL, MOSQ_LOG_ERR, "Error setting groups whilst dropping privileges: %s.", err);
				return 1;
			}
			rc = setgid(pwd->pw_gid);
			if(rc == -1){
				err = strerror(errno);
				log__printf(NULL, MOSQ_LOG_ERR, "Error setting gid whilst dropping privileges: %s.", err);
				return 1;
			}
			rc = setuid(pwd->pw_uid);
			if(rc == -1){
				err = strerror(errno);
				log__printf(NULL, MOSQ_LOG_ERR, "Error setting uid whilst dropping privileges: %s.", err);
				return 1;
			}
		}
		if(geteuid() == 0 || getegid() == 0){
			log__printf(NULL, MOSQ_LOG_WARNING, "Warning: Mosquitto should not be run as root/administrator.");
		}
	}
#else
	UNUSED(config);
#endif
	return MOSQ_ERR_SUCCESS;
}

static void mosquitto__daemonise(void)
{
#ifndef WIN32
	char *err;
	pid_t pid;

	pid = fork();
	if(pid < 0){
		err = strerror(errno);
		log__printf(NULL, MOSQ_LOG_ERR, "Error in fork: %s", err);
		exit(1);
	}
	if(pid > 0){
		exit(0);
	}
	if(setsid() < 0){
		err = strerror(errno);
		log__printf(NULL, MOSQ_LOG_ERR, "Error in setsid: %s", err);
		exit(1);
	}

	assert(freopen("/dev/null", "r", stdin));
	assert(freopen("/dev/null", "w", stdout));
	assert(freopen("/dev/null", "w", stderr));
#else
	log__printf(NULL, MOSQ_LOG_WARNING, "Warning: Can't start in daemon mode in Windows.");
#endif
}


static void signal__setup(void)
{
	signal(SIGINT, handle_sigint);
	signal(SIGTERM, handle_sigint);
#ifdef SIGHUP
	signal(SIGHUP, handle_sighup);
#endif
#ifndef WIN32
	signal(SIGUSR1, handle_sigusr1);
	signal(SIGUSR2, handle_sigusr2);
	signal(SIGPIPE, SIG_IGN);
#endif
#ifdef WIN32
	CreateThread(NULL, 0, SigThreadProc, NULL, 0, NULL);
#endif
}


static int pid__write(void)
{
	FILE *pid;

	if(db.config->pid_file){
		pid = mosquitto__fopen(db.config->pid_file, "wt", false);
		if(pid){
			fprintf(pid, "%d", getpid());
			fclose(pid);
		}else{
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to write pid file.");
			return 1;
		}
	}
	return MOSQ_ERR_SUCCESS;
}


static void report_features(void)
{
#ifdef WITH_BRIDGE
	log__printf(NULL, MOSQ_LOG_INFO, "Bridge support available.");
#else
	log__printf(NULL, MOSQ_LOG_INFO, "Bridge support NOT available.");
#endif
#ifdef WITH_PERSISTENCE
	log__printf(NULL, MOSQ_LOG_INFO, "Persistence support available.");
#else
	log__printf(NULL, MOSQ_LOG_INFO, "Persistence support NOT available.");
#endif
#ifdef WITH_TLS
	log__printf(NULL, MOSQ_LOG_INFO, "TLS support available.");
#else
	log__printf(NULL, MOSQ_LOG_INFO, "TLS support NOT available.");
#endif
#ifdef FINAL_WITH_TLS_PSK
	log__printf(NULL, MOSQ_LOG_INFO, "TLS-PSK support available.");
#else
	log__printf(NULL, MOSQ_LOG_INFO, "TLS-PSK support NOT available.");
#endif
#ifdef WITH_WEBSOCKETS
	log__printf(NULL, MOSQ_LOG_INFO, "Websockets support available.");
#else
	log__printf(NULL, MOSQ_LOG_INFO, "Websockets support NOT available.");
#endif
}


int main(int argc, char *argv[])
{
	struct mosquitto__config config;
	int rc;
#ifdef WIN32
	SYSTEMTIME st;
#else
	struct timeval tv;
#endif
	struct mosquitto *ctxt, *ctxt_tmp;

#if defined(WIN32) || defined(__CYGWIN__)
	if(argc == 2){
		if(!strcmp(argv[1], "run")){
			service_run();
			return 0;
		}else if(!strcmp(argv[1], "install")){
			service_install();
			return 0;
		}else if(!strcmp(argv[1], "uninstall")){
			service_uninstall();
			return 0;
		}
	}
#endif


#ifdef WIN32
	GetSystemTime(&st);
	srand(st.wSecond + st.wMilliseconds);
#else
	gettimeofday(&tv, NULL);
	srand((unsigned int)(tv.tv_sec + tv.tv_usec));
#endif

#ifdef WIN32
	if(_setmaxstdio(8192) != 8192){
		/* Old limit was 2048 */
		if(_setmaxstdio(2048) != 2048){
			log__printf(NULL, MOSQ_LOG_WARNING, "Warning: Unable to increase maximum allowed connections. This session may be limited to 512 connections.");
		}
	}

#endif

	memset(&db, 0, sizeof(struct mosquitto_db));
	db.now_s = mosquitto_time();
	db.now_real_s = time(NULL);

	net__broker_init();

	config__init(&config);
	rc = config__parse_args(&config, argc, argv);
	if(rc != MOSQ_ERR_SUCCESS) return rc;
	db.config = &config;

	/* Drop privileges permanently immediately after the config is loaded.
	 * This requires the user to ensure that all certificates, log locations,
	 * etc. are accessible my the `mosquitto` or other unprivileged user.
	 */
	rc = drop_privileges(&config);
	if(rc != MOSQ_ERR_SUCCESS) return rc;

	if(config.daemon){
		mosquitto__daemonise();
	}

	if(pid__write()) return 1;

	rc = db__open(&config);
	if(rc != MOSQ_ERR_SUCCESS){
		log__printf(NULL, MOSQ_LOG_ERR, "Error: Couldn't open database.");
		return rc;
	}

	/* Initialise logging only after initialising the database in case we're
	 * logging to topics */
	if(log__init(&config)){
		rc = 1;
		return rc;
	}
	log__printf(NULL, MOSQ_LOG_INFO, "mosquitto version %s starting", VERSION);
	if(db.config_file){
		log__printf(NULL, MOSQ_LOG_INFO, "Config loaded from %s.", db.config_file);
	}else{
		log__printf(NULL, MOSQ_LOG_INFO, "Using default config.");
	}
	report_features();

	rc = mosquitto_security_module_init();
	if(rc) return rc;
	rc = mosquitto_security_init(false);
	if(rc) return rc;

	/* After loading persisted clients and ACLs, try to associate them,
	 * so persisted subscriptions can start storing messages */
	HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp){
		if(ctxt && !ctxt->clean_start && ctxt->username){
			rc = acl__find_acls(ctxt);
			if(rc){
				log__printf(NULL, MOSQ_LOG_WARNING, "Failed to associate persisted user %s with ACLs, "
					"likely due to changed ports while using a per_listener_settings configuration.", ctxt->username);
			}
		}
	}

#ifdef WITH_SYS_TREE
	sys_tree__init();
#endif

	if(listeners__start()) return 1;

	rc = mux__init(g_listensock, g_listensock_count);
	if(rc) return rc;

	signal__setup();

#ifdef WITH_BRIDGE
	bridge__start_all();
#endif

	log__printf(NULL, MOSQ_LOG_INFO, "mosquitto version %s running", VERSION);
#ifdef WITH_SYSTEMD
	sd_notify(0, "READY=1");
#endif

	g_run = 1;
	rc = mosquitto_main_loop(g_listensock, g_listensock_count);

	log__printf(NULL, MOSQ_LOG_INFO, "mosquitto version %s terminating", VERSION);

	/* FIXME - this isn't quite right, all wills with will delay zero should be
	 * sent now, but those with positive will delay should be persisted and
	 * restored, pending the client reconnecting in time. */
	HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp){
		context__send_will(ctxt);
	}
	will_delay__send_all();

#ifdef WITH_PERSISTENCE
	persist__backup(true);
#endif
	session_expiry__remove_all();

	listeners__stop();

	HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp){
#if WITH_WEBSOCKETS == WS_IS_LWS
		if(!ctxt->wsi)
#endif
		{
			context__cleanup(ctxt, true);
		}
	}
	HASH_ITER(hh_sock, db.contexts_by_sock, ctxt, ctxt_tmp){
		context__cleanup(ctxt, true);
	}
#ifdef WITH_BRIDGE
	bridge__db_cleanup();
#endif
	context__free_disused();

#ifdef WITH_TLS
	mosquitto__free(db.tls_keylog);
#endif
	db__close();

	mosquitto_security_module_cleanup();

	if(config.pid_file){
		(void)remove(config.pid_file);
	}

	log__close(&config);
	config__cleanup(db.config);
	net__broker_cleanup();

	return rc;
}

#ifdef WIN32
int WINAPI WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance, LPSTR lpCmdLine, int nCmdShow)
{
	char **argv;
	int argc = 1;
	char *token;
	char *saveptr = NULL;
	int rc;

	UNUSED(hInstance);
	UNUSED(hPrevInstance);
	UNUSED(nCmdShow);

	argv = mosquitto__malloc(sizeof(char *)*1);
	argv[0] = "mosquitto";
	token = strtok_r(lpCmdLine, " ", &saveptr);
	while(token){
		argc++;
		argv = mosquitto__realloc(argv, sizeof(char *)*argc);
		if(!argv){
			fprintf(stderr, "Error: Out of memory.\n");
			return MOSQ_ERR_NOMEM;
		}
		argv[argc-1] = token;
		token = strtok_r(NULL, " ", &saveptr);
	}
	rc = main(argc, argv);
	mosquitto__free(argv);
	return rc;
}
#endif
