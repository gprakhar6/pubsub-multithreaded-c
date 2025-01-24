#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "pubsub.h"

topic_t *t;
subscriber_t *s;

void* publisher(void *arg)
{
    uint64_t data = 0;
    while(1) {
	data++;
	pub_data(&data, t);
	if(data > 1000000000)
	    goto ret;
    }
ret:
    printf("exiting pub\n");
    return NULL;
}

void* poller(void *arg)
{
    uint64_t data = 0, pdata = 0;
    int fail;
    while(1) {
	if(!(fail = poll_data(&data, s))) {
	    if(data < pdata) {
		printf("BAD DATA fail = %d %lu %lu\n", fail, data, pdata);
		printf("s->next = %lu s->tail=%u\n", s->next_rd_count, s->tail_ptr);
		exit(0);
	    }
	    pdata = data;
	    if(data > 1000000000)
		goto ret;
	}
    }
ret:
    return NULL;
}
uint64_t d;
int main()
{
    pthread_t tid1,tid2;
    init_pubsub();
    t = allocate_topic("data", sizeof(d), 1000);
    s = allocate_subscriber("data", RESET_TO_TAIL_VALUE);

    pthread_create(&tid1, NULL, publisher, NULL);
    pthread_create(&tid1, NULL, poller, NULL);

    pthread_join(tid1, NULL);
    pthread_join(tid2, NULL);
    return 0;
}
