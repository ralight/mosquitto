#ifndef BROKER_FUZZ_H
#define BROKER_FUZZ_H

#define kMinInputLength 5
#define kMaxInputLength 10000

struct fuzz_data{
	uint8_t *data;
	size_t size;
	uint16_t port;
};

void *run_broker(void *args);
void recv_timeout(int sock, void *buf, size_t len, int timeout_us);
int connect_retrying(int port);
void run_client(struct fuzz_data *fuzz);

#endif
