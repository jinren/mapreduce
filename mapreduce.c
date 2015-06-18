/******************************************************************
* Description: Count words frequency with map-reduce.
* 
* Created Date: 06/17/2015
* Author: Jin Ren (renjin77@hotmail.com)
*
******************************************************************/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <search.h>
#include <sys/queue.h>
#include <pthread.h>

#define MAX_PARALLELIZATION     1024           /* Max threads */
#define READ_SIZE               4096           /* Read stride size */
#define READ_BUF_SIZE           READ_SIZE+1    /* Read buf size */
#define MAX_WORD_LENGTH			READ_SIZE      /* Max length of a word */
#define RESULT_FILE             "result"       /* Result file name */

/* Store information about a word */
typedef struct _word_info_t{
	char *buf;                                /* String buffer */
	long long count;                          /* Word count */
	SLIST_ENTRY(_word_info_t) link;           /* Collision list */
}WORD_INFO_T;

/* Tree node */
typedef struct _node_t{
	long long key;                            /* Hash key of a word */
	SLIST_HEAD(listhead, _word_info_t) head;  /* Collision list of words that have the same key */
}NODE_T;

/* Large strings may span multiple read buffer. Use buf_unit_t to link them together. */
typedef struct _buf_unit_t {
	SLIST_ENTRY(_buf_unit_t) link;
	char buf[1];
}BUF_UNIT_T;

/* Keep track of large strings that span multiple read strides. */
typedef struct _parse_info_t {
	int id;                                    /* Debug id for pthread */
	long long size;                            /* Cummulated size from previous read */
	long long end;                             /* Ending offset + 1 */
	SLIST_HEAD(parsehead, _buf_unit_t) head;   /* Linked string buffer */
}PARSE_INFO_T;

/* Parameters for each thread. */
typedef struct _worker_t{
	int id;                                    /* Thread id */
	void *md_info;                             /* Map-reduce information */
	long long start;                           /* Starting offset */
	long long end;                             /* Ending offset */
	pthread_t thread;                          /* Thread info */
	int active;                                /* Thread created successfully */
	void *root;                                /* Tree for word count */
}WORKER_T;

/* Information of map reduce operation. */
typedef struct _mapreduce_info_t {
	char *file_name;                           /* Source file name */
	int parallelization;                       /* Parallelization */
	int n_thread;                              /* Number of threads needed */
	int n_active_thread;                       /* Number of thread created */
	WORKER_T *workers;                         /* Workers array */
	void *root;                                /* Final result tree root */
}MAPREDUCE_INFO_T;

char separators[] = {" \t\n\r{}[]()<>\'\"?!.,-%%@~`*^$#_=+/\\&:;|"};
MAPREDUCE_INFO_T g_md_info;

int add_word(void **root, char *buf, PARSE_INFO_T *parse_info);

/******************************************************************************

Allocate new tree node with one word_info_t.

 *****************************************************************************/
NODE_T *new_node(int len)
{
	NODE_T *node;
	WORD_INFO_T *word_info;
	if (NULL == (node = malloc(sizeof(NODE_T)))) {
		printf("%s: failed to allocate node\n", __func__);
		exit(-1);
	}
	if (NULL == (word_info = malloc(sizeof(WORD_INFO_T)))) {
		printf("%s: failed to allocate word info\n", __func__);
		exit(-1);
	}
	if (NULL == (word_info->buf = malloc(len + 1))) {
		printf("%s: failed to allocate buffer\n", __func__);
		exit(-1);
	}
	memset(word_info->buf, 0, len + 1);
	SLIST_INIT(&node->head);
	SLIST_INSERT_HEAD(&node->head, word_info, link); 
	return node;
}

/******************************************************************************

Used for tree insertion to compare the key of the new node and existing nodes.
It returns 0, a positive number, or a negative number.
It also merges the word_list of two nodes.

 *****************************************************************************/
int node_compare(const void *node1, const void *node2)
{
	NODE_T *node_new = (NODE_T*)node1;
	NODE_T *node_old = (NODE_T*)node2;
	WORD_INFO_T *word_info, *word_info_new;
	int flag = 0;
	long long result;

	result = node_new->key - node_old->key;
	if (0 == result) {
		while (!SLIST_EMPTY(&node_new->head)) {
			word_info_new = (WORD_INFO_T*)SLIST_FIRST(&node_new->head);
			SLIST_REMOVE_HEAD(&node_new->head, link);
			SLIST_FOREACH(word_info, &node_old->head, link){
				if (0 == strncmp(word_info->buf, word_info_new->buf, strlen(word_info->buf))) {
					word_info->count += word_info_new->count;
					//printf("inc old %s, %d\n", word_info->buf, word_info->count);
					free(word_info_new->buf);
					free(word_info_new); 
					flag = 1;
					break;
				}
			}
			if (flag == 0) {
				SLIST_INSERT_HEAD(&node_old->head, word_info_new, link);
				//printf("collision %s, %d\n", word_info->buf, word_info->count);
			}
		}
	}

	return result;
}

/******************************************************************************

Print a tree node's word list.

 *****************************************************************************/
void print_node(const void *node1, const VISIT order, const int level)
{
	NODE_T *node = *(NODE_T**)node1;
	WORD_INFO_T *word_info;
	
	if (order == postorder || order == leaf) {
		SLIST_FOREACH(word_info, &node->head, link) {
			printf("%s: %lld\n", word_info->buf, word_info->count); 
		}
	} 
}

/******************************************************************************

Save the tree node information to the result file.

 *****************************************************************************/
void save_node(const void *node1, const VISIT order, const int level)
{
	NODE_T *node = *(NODE_T**)node1;
	WORD_INFO_T *word_info;
	FILE *fp;
	char buf[256];
	
	if (order == postorder || order == leaf) {
		SLIST_FOREACH(word_info, &node->head, link) {
			fp = fopen(RESULT_FILE, "a+");
			if (fp == NULL) {
				printf("failed to open %s\n", RESULT_FILE);
				return;
			} else {
				fseeko64(fp, 0L, SEEK_END);
				fputs(word_info->buf, fp);
				snprintf(buf, 256, ": %lld\n", word_info->count);
				fputs(buf, fp);
				fclose(fp);
			}
		}
	} 
}

/******************************************************************************

Free a tree node.

 *****************************************************************************/
void free_node(void *node1) 
{
	NODE_T *node = (NODE_T*)node1;
	WORD_INFO_T *word_info;

	while (!SLIST_EMPTY(&node->head)) {
		word_info = (WORD_INFO_T*)SLIST_FIRST(&node->head);
		SLIST_REMOVE_HEAD(&node->head, link);
		free(word_info->buf);
		free(word_info); 
	} 
	free(node);
	
	return; 
}

/******************************************************************************

Merge one tree node with the final result tree. If the same key found, the word
list is merged and the tree node is freed, otherwise the node will be inserted
into the final result tree.

 *****************************************************************************/
void merge_node(void *node1) 
{
	NODE_T *node = (NODE_T*)node1;
	MAPREDUCE_INFO_T *md_info = &g_md_info;
	NODE_T **node_out;

	node_out = tsearch(node, &md_info->root, node_compare);
	if (NULL == node_out) {
		printf("%s: failed to call tsearch\n", __func__);
		return;
	} else if (*node_out != node) {
		free_node(node);
	}
	
	return; 
}

/******************************************************************************

Walk through the tree and show result.

 *****************************************************************************/
void show_result(void *root)
{
	printf("\n******Finial result******\n");
	twalk(root, print_node);
}

/******************************************************************************

Show the result tree of each map thread.

 *****************************************************************************/
void show_intermediate_result(MAPREDUCE_INFO_T *md_info)
{
	int i;
	WORKER_T *worker;

	printf("\n>>>>start of intermediate result>>>>\n");
	for(i = 0; i < md_info->n_thread; i++) {
		worker = &md_info->workers[i];
		if (worker->root != NULL) {
			printf("thread %d, start %lld, end %lld:\n", worker->id, worker->start, worker->end);
			show_result(worker->root);
		}
	}
	printf("<<<<end of intermediate result<<<<\n\n");
}

/******************************************************************************

Walk through a tree and save its result to a file.

 *****************************************************************************/
int save_result(void *root)
{
	remove(RESULT_FILE);
	twalk(root, save_node);
	return 0;
}

/******************************************************************************

Free a tree.

 *****************************************************************************/
int free_tree(void *root)
{
	tdestroy(root, free_node);
	return 0;
}

/******************************************************************************

When a large string extends to the next read buffer, save the current part into
a linked list in parse_info_t.

 *****************************************************************************/
int add_pending(PARSE_INFO_T *parse_info, char *buf, long long offset)
{
	BUF_UNIT_T *buf_unit, *unit, *next;
	if (NULL == (buf_unit = malloc(sizeof(BUF_UNIT_T) + strlen(buf)))) {
		printf("failed to malloc\n");
		return -1;
	}

	memset(buf_unit->buf, 0, strlen(buf) + 1);
	strncpy(buf_unit->buf, buf, strlen(buf));
	unit = SLIST_FIRST(&parse_info->head);
	while(unit) {
		next = SLIST_NEXT(unit, link);
		if (!next) break;
		unit = next;
	}
	if (!unit)
		SLIST_INSERT_HEAD(&parse_info->head, buf_unit, link);
	else
		SLIST_INSERT_AFTER(unit, buf_unit, link);
	parse_info->size += strlen(buf);
	parse_info->end = offset;

	return 0;
}

/******************************************************************************

Get the remaining part comes from last read from parse_info_t.

 *****************************************************************************/
int get_pending(PARSE_INFO_T *parse_info, char *buf)
{
	BUF_UNIT_T *buf_unit;
	while (!SLIST_EMPTY(&parse_info->head)) {
		buf_unit = (BUF_UNIT_T*)SLIST_FIRST(&parse_info->head);
		SLIST_REMOVE_HEAD(&parse_info->head, link);
		strcat(buf, buf_unit->buf);
		free(buf_unit); 
	}
	parse_info->size = 0;
	return 0;
}

/******************************************************************************

Process the pending word if :
1. Current string buf is not a contiguous part of the pending string.
2. The combined word length is larger than MAX_WORD_LENGTH. This is a temporary
   measure before any method is implemented to handle extreme large strings.

 *****************************************************************************/
int check_pending(void **root, char *buf, PARSE_INFO_T *parse_info, long long offset)
{
	if (parse_info->size > 0) {
		if (parse_info->end != offset || parse_info->size > MAX_WORD_LENGTH ||
			(parse_info->end == offset && parse_info->size + strlen(buf) > MAX_WORD_LENGTH)) {
			if(0 != add_word(root, "", parse_info)) {
				printf("%s: failed to call add_word\n", __func__);
				return -1;
			}
		}
	}
	return 0;
}


/******************************************************************************

Convert all upper case letters to lower case.

 *****************************************************************************/
void to_lower(char *buf, size_t len)
{
	size_t i;
	for(i = 0; i < len; i++){
		if (buf[i] >= 'A' && buf[i] <= 'Z')
			buf[i] = tolower(buf[i]);
	}
}

/******************************************************************************

Generate a 32-bit key for tree.

 *****************************************************************************/
unsigned int get_key(char *buf, size_t len)
{
	unsigned int q = len/sizeof(unsigned int),
	r = len%sizeof(unsigned int),
	*p = (unsigned int*)buf;
	unsigned int crc = 0x11111111;
 
	while (q--) {
		__asm__ __volatile__(
			".byte 0xf2, 0xf, 0x38, 0xf1, 0xf1;"
			:"=S"(crc)
			:"0"(crc), "c"(*p)
		);
		p++;
	}
 
	buf= (char*)p;
	while (r--) {
		__asm__ __volatile__(
			".byte 0xf2, 0xf, 0x38, 0xf0, 0xf1"
			:"=S"(crc)
			:"0"(crc), "c"(*buf)
		);
		buf++;
	}
 
	return crc;
}

/******************************************************************************

Add one word into the tree. If the key already exists, free the new node.

 *****************************************************************************/
int add_word(void **root, char *buf, PARSE_INFO_T *parse_info)
{
	size_t size;
	NODE_T *node, **node_out;
	WORD_INFO_T *word_info;

	size = strlen(buf) + parse_info->size;
	if (NULL == (node = new_node(size))) {
		printf("failed to call new_node\n");
		return -1;
	}

	word_info = (WORD_INFO_T*)SLIST_FIRST(&node->head); 
	get_pending(parse_info, word_info->buf);
	strncat(word_info->buf, buf, size);
	to_lower(word_info->buf, size);
	word_info->count = 1;
	node->key = get_key(word_info->buf, size);
	//printf("[%d]: word=%s, key=%llx\n", parse_info->id, word_info->buf, node->key);
	node_out = tsearch(node, root, node_compare);
	if (NULL == node_out) {
		printf("failed to call tsearch\n");
		return -1;
	} else if (*node_out != node) {
		free_node(node);
	}

	return 0;
}

/******************************************************************************

Parsing the read buffer to get word.

 *****************************************************************************/
int parse_buf(void **root, char *buf, int len, PARSE_INFO_T *parse_info, long long offset)
{
	int size;
	char *start, *savedptr;

	start = strtok_r(buf, separators, &savedptr);
	while(start != NULL) {
		size = strlen(start);
		if(0 != check_pending(root, start, parse_info, offset + start - buf)) {
			printf("failed to call check_pending\n");
			return -1;
		}
		if (start + size < buf + len) {
			// Not reaching the end of buf
			if(0 != add_word(root, start, parse_info)) {
				printf("failed to call add_word\n");
				return -1;
			}
		} else {
			// Reached the end of buf, the string could span to next buf.
			if(0 != add_pending(parse_info, start, offset + len)) {
				printf("failed to call add_pending\n");
				return -1;
			}
		}
		start = strtok_r(NULL, separators, &savedptr);
	}

	return 0;
}


/******************************************************************************

Read the input file with specified offset. Each time consume READ_SIZE.

 *****************************************************************************/
int map(WORKER_T *worker, char *name)
{
	FILE *fp;
	char *buf;
	int len;
	long long size, start;
	PARSE_INFO_T parse_info;

	if (NULL == (buf = malloc(READ_BUF_SIZE))) {
		printf("%s failed to allocate buffer\n",__func__);
		return -1;
	}
	memset(buf, 0, READ_BUF_SIZE);
	SLIST_INIT(&parse_info.head);
	parse_info.size = 0;
	parse_info.id = worker->id;
	size = worker->end - worker->start + 1;
	start = worker->start;
	
	fp = fopen(name, "r");
	if (fp == NULL) {
		printf("failed to open %s\n", name);
		return -1;
	}

	if (0 != fseeko64(fp, worker->start, SEEK_SET)) {
		printf("failed to seek to %lld %s\n", worker->start, name);
		fclose(fp);
		free(buf);
		return -1;
	}
	
	while(size > 0 && (0 < (len = fread(buf, 1, READ_SIZE, fp)))) {
		if (len > size)
			len = size;
		buf[len] = 0;
		//printf("[%d]: %s, len=%d, size=%lld\n", parse_info.id, buf, len, size);
		parse_buf(&worker->root, buf, len, &parse_info, start);
		memset(buf, 0, READ_BUF_SIZE);
		size -= len;
		start += len;
	}

	if (parse_info.size) {
		memset(buf, 0, READ_BUF_SIZE);
		if(0 != add_word(&worker->root, buf, &parse_info)) {
			printf("%s: failed to call add_word\n", __func__);
			fclose(fp);
			free(buf);
			return -1;
		}
	}

	fclose(fp);
	free(buf);
	return 0;
}

/******************************************************************************

Find the next separator.

 *****************************************************************************/
char* next_separator(char *buf, int len)
{
	int i, j;
	int size = sizeof(separators)/sizeof(char);

	for(i = 0; i < len; i++) {
		for(j = 0; j < size; j++) {
			if (buf[i] == separators[j])
				return buf + i;
		}
	}
	return NULL;
}

/******************************************************************************

Find the starting and ending offset of the next chunk for mapping operation.
Avoid splitting one word into two chunks.

 *****************************************************************************/
int next_chunk(FILE *fp, long long curr, long long stride, long long *start, long long *end)
{
	char *buf;
	char *ptr = NULL, *savedptr;
	int ret = -1, len;
	
	if (NULL == (buf = malloc(READ_BUF_SIZE))) {
		printf("%s failed to allocate buffer\n",__func__);
		exit(-1);
	}
	memset(buf, 0, READ_BUF_SIZE);
	*start = -1;
	*end = -1;

	if (0 == fseeko64(fp, curr, SEEK_SET)) {
		while((len = fread(buf, 1, READ_SIZE, fp)) > 0) {
			buf[len] = 0;
			//printf("%s\n", buf);
			ptr = strtok_r(buf, separators, &savedptr);
			if (ptr)
				break;
			curr += strlen(buf);
			memset(buf, 0, READ_BUF_SIZE);
		}
		
		if (ptr) {
			*start = (long long)(ptr - buf) + curr;
			*end = *start + stride;
			curr = *end;
			if (0 == fseeko64(fp, curr, SEEK_SET)) {
				while((len = fread(buf, 1, READ_SIZE, fp)) > 0) {
					buf[len] = 0;
					ptr = next_separator(buf, strlen(buf));
					if (ptr)
						break;
					curr += READ_SIZE;
					memset(buf, 0, READ_BUF_SIZE);
				}
				if (ptr) {
					*end = (long long)(ptr - buf) + curr - 1;
				}
			}
			ret = 0;
		}
	}
	
	free(buf);
	return ret;
}

/******************************************************************************

The main body of one mapping thread.

 *****************************************************************************/
void *map_worker(void *ptr)
{
	WORKER_T *worker = (WORKER_T*)ptr;
	MAPREDUCE_INFO_T *md_info = (MAPREDUCE_INFO_T*)worker->md_info;

	__sync_fetch_and_add(&md_info->n_active_thread, 1);
	if (0 != map(worker, md_info->file_name)) {
		printf("thread %d:failed to call map\n", worker->id);
	}
	__sync_fetch_and_sub(&md_info->n_active_thread, 1);
	
	pthread_exit(NULL);
}

/******************************************************************************

Create a mapping thread worker and set parameters.

 *****************************************************************************/
int start_worker(MAPREDUCE_INFO_T *md_info, long long start, long long end)
{
	WORKER_T *worker;
	int ret;

	worker = &md_info->workers[md_info->n_thread];
	worker->id = md_info->n_thread++;
	worker->start = start;
	worker->end = end;
	worker->md_info= md_info;

	ret = pthread_create(&worker->thread, NULL, map_worker, (void*)worker);
	if(ret) {
		printf("failed to start thread %d\n", worker->id);
		return -1;
	} else
		worker->active = 1;
	
	return 0;
}

/******************************************************************************

Wait for all mapping works to finish analyzing.

 *****************************************************************************/
int wait_workers(MAPREDUCE_INFO_T *md_info)
{
	int i, n_thread = md_info->n_thread;
	WORKER_T *worker;

	printf("Waiting for map workers to finish......\n");
	for(i = 0; i < n_thread; i++) {
		worker = &md_info->workers[i];
		if (worker->active)
			pthread_join(worker->thread, NULL);
	}
	printf("Map workers finished.\n");
	
	return 0;
}

/******************************************************************************

Split source file into chunks, and assign each chunk to a mapping worker thread.

 *****************************************************************************/
int start_map(char *name, int parallelization, MAPREDUCE_INFO_T *md_info)
{
	long long size, stride;
	long long start, end = 0;
	FILE *fp;
	
	fp = fopen(name, "r");
	if (fp == NULL) {
		printf("failed to open %s\n", name);
		exit(-1);
	}
	fseeko64(fp, 0L, SEEK_END);
	size = ftello64(fp);

	stride = size/parallelization;

	if (NULL == (md_info->workers = malloc(sizeof(WORKER_T) * parallelization))) {
		printf("failed to allocate workers\n");
		exit(-1);
	}
	memset(md_info->workers, 0, sizeof(WORKER_T) * parallelization);
	md_info->root = NULL;
	md_info->file_name = name;
	md_info->parallelization = parallelization;
	md_info->n_thread = 0;
	md_info->n_active_thread = 0;

	while(0 == next_chunk(fp, end, stride, &start, &end)) {
		//printf("start = %lld, end=%lld\n", start, end);
		if (0 != start_worker(md_info, start, end)) {
			printf("failed to start worker at %lld\n", start);
			exit(-1);
		}
		end++;
	}

	fclose(fp);
	return 0;
}

/******************************************************************************

Merge the tree of a mapping thread to the final result tree.

 *****************************************************************************/
int reduce(void **root, void *sub_root)
{
	tdestroy(sub_root, merge_node);
	return 0;
}

/******************************************************************************

Merge the result of all mapping works into the final result tree.

 *****************************************************************************/
int start_reduce(MAPREDUCE_INFO_T *md_info)
{
	int i, n_thread = md_info->n_thread;
	WORKER_T *worker;

	for(i = 0; i < n_thread; i++) {
		worker = &md_info->workers[i];
		if (worker->root != NULL) {
			reduce(&md_info->root, worker->root);
			worker->root = NULL;
		}
	}
	return 0;
}

/******************************************************************************

Free resources.

 *****************************************************************************/
void clean_up(MAPREDUCE_INFO_T *md_info)
{
	int i;
	WORKER_T *worker;

	for(i = 0; i < md_info->n_thread; i++) {
		worker = &md_info->workers[i];
		if (worker->root != NULL)
			free_tree(worker->root);
	}

	if (md_info->root != NULL)
		free_tree(md_info->root);
	
	free(md_info->workers);
}

void usage(char *app)
{
	printf("\nError: Missing parameters.\n");
	printf("Usage: %s filename parallelization\n\n", app);
	exit(-1);
}

int main(int argc, char *argv[])
{
	int parallelization;
	char *name;
	MAPREDUCE_INFO_T *md_info = &g_md_info;
	int show = 1;

	if (argc < 3) {
		usage(argv[0]);
	}
	
	parallelization = atoi(argv[2]);
	if (argc == 4 && 0 == strcmp(argv[3], "show=0"))
		show = 0;
	
	name = realpath(argv[1], NULL);
	if (NULL == name) {
		printf("Failed to resolve path for %s\n", argv[1]);
		exit(-1);
	}
	if (parallelization < 0 || parallelization > MAX_PARALLELIZATION) {
		printf("Invalid parallelization %d which should be between 1 and %d\n", parallelization, MAX_PARALLELIZATION);
		exit(-1);
	}
	printf("start processing %s with %d threads\n", name, parallelization);

	start_map(name, parallelization, md_info);

	wait_workers(md_info);

	//show_intermediate_result(md_info);

	start_reduce(md_info);

	if (show) show_result(md_info->root);

	save_result(md_info->root);

	clean_up(md_info);
	
	free(name);

	return 0;
}

