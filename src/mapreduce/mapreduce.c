#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#define STDIN 0
#define STDOUT 1
#define STDERR 2

#define READ 0
#define WRITE 1

#define MAXCOM 1
#define BUFSIZE 1024

typedef struct keys{
	char *key;
	struct keys *next;
} Keys;

typedef struct task{
	int pid;
	int canos[2];
	struct task *next;
} Task;

typedef struct waitList{
	char *cmd;
	struct waitList *next;
} WaitList;


Task *running; 
WaitList *waiting;

char *map;
Keys *keys;
int tasks_running;
int tasks_waiting;

int exists_key(char *key){
	for(Keys *aux = keys; aux; aux = aux->next){
		if(!strcmp(key, aux->key)) return 1;
	}
	return 0;
}


void insert_key(char *key){
	if(!exists_key(key)){
		Keys *new = (Keys*)malloc(sizeof(key));
		new->key = strdup(key);
		new->next = keys;
		keys = new;
	}
}

void reduce(char *reduce){
	char buffer[100];
	char buf2[50];
	memset(buffer, 0, 100);
	memset(buf2, 0, 50);
	int canos1[2];
	int canos2[2];
	int count;
	for(Keys *aux = keys; aux; aux = aux->next){
		FILE *fp = fopen(aux->key, "r");
		pipe(canos1);
		pipe(canos2);
		memset(buffer, 0, 100);
		memset(buf2, 0, 50);



		if(!fork()){
			dup2(canos1[WRITE], STDOUT);
			close(canos1[READ]);

			dup2(canos2[READ], STDIN);
			close(canos2[WRITE]);

			execlp(reduce, reduce, NULL);
		}

		close(canos1[WRITE]);
		close(canos2[READ]);

		while((count = read(fileno(fp), buffer, 99))){
			printf("reducing %s\n", buffer);
			write(canos2[WRITE], buffer, strlen(buffer));
		}

		read(canos1[READ], buf2, 49);
		printf("REDUCTION k:%s v:%s\n", aux->key, buf2);
		fclose(fp);
		//close(canos1[WRITE]);
		close(canos1[READ]);
		close(canos2[WRITE]);
		//close(canos2[READ]);
	}


}

char* pop_waiting(){
	char *cmd = waiting->cmd;
	WaitList *tmp = waiting;
	waiting = waiting->next;
	free(tmp);
	tasks_waiting--;
	return cmd;
}


void push_waiting(char *cmd){
	WaitList *new = (WaitList*)malloc(sizeof(WaitList));
	new->cmd = cmd;
	new->next = waiting;
	waiting = new;

	tasks_waiting++;
	//printf("WAIT %s %s\n", map, cmd);
}


Task* get_and_remove_running(int pid){
	Task *prev = running;

	if(prev->pid == pid){
		running = prev->next;
		return prev;
	}

	for(Task *aux = prev->next; aux; aux = aux->next){

		if(aux->pid == pid){
			prev->next = aux->next;
			return aux;
		}
		prev = aux;
	}
	return NULL;
}

void run_task(char *cmd){	
	Task *new = (Task*)malloc(sizeof(Task));
	pipe(new->canos);
	new->next = running;
	running = new;
	//printf("RUN %s %s\n", map, cmd);

	int pid = fork();
	if(!pid){
		close(new->canos[READ]);
		dup2(new->canos[WRITE], 1);
		execlp(map, map, cmd, NULL);
	}
	close(new->canos[WRITE]);
	new->pid = pid;
	tasks_running++;

}

void child_handler(){
	int status;
	
	char buffer[100];
	char key[10];
	char value[90];

	while(1){
		int pid = wait(&status);
		//printf("waited for pid %d\n", pid);
		if(pid <= 0) break;
		memset(buffer, 0, 100);
		memset(key, 0, 10);
		memset(value, 0, 90);

		Task *t = get_and_remove_running(pid);
		if(!t){
			printf("FATAL: PID NOT FOUND IN LIST\n");
			exit(1);
		}
		//printf("found Task %d\n", t->pid);

		read(t->canos[READ], buffer, 99);
		//printf("UNPARSED: %s\n", buffer);
		sscanf(buffer, "%s %s", key, value);
		//printf("RECV %d: k:%s v:%s\n", pid, key, value);

		/*char *tmp = get_ht(results, key);
		if(!tmp){
			insert_ht(results, strdup(key), strdup(value));
		}
		else{
			remove_ht(results, key);
			printf("before: (%s) (%s)\n", tmp, value);
			//realloc(tmp, strlen(tmp) + strlen(value));
			//sprintf(tmp, "%s%s", tmp, value);
			printf("after: (%s) (%s)\n", tmp, value);

			insert_ht(results, strdup(key), tmp);
		}*/
		FILE *f = fopen(key, "a+");
		if(!f){
			printf("FATAL: Could not open file\n");
			exit(1);
		}
		fprintf(f, "%s\n", value);
		fclose(f);

		insert_key(key);

		close(t->canos[READ]);
		close(t->canos[WRITE]);
		free(t);

		tasks_running--;
		if(tasks_waiting) run_task(pop_waiting());
	}
}

void init_system(char *argv[]){
	running = NULL;
	waiting = NULL;

	tasks_running = 0;
	tasks_waiting = 0;

	map = argv[1];
	keys = NULL;
}

int main(int argc, char* argv[]){
	int i = 0;
	int count = 0;

	if(argc != 3){
		write(STDOUT, "Error: invalid number of arguments\n", 35);
		return 1;
	}

	init_system(argv);
	char *buffer = (char*)calloc(BUFSIZE, sizeof(char));
	signal(SIGCHLD, child_handler);
	char cenas[] = "/Users/beatgodes/sandbox//test.c";
	// ler caracter a caracter, separar por \n
	// quando count == 0, sai do ciclo porque chegou a EOF
	while((count = read(STDIN, &(buffer[i]), 1))){
		//printf("count %d char (%c)\n", count, buffer[i]);
		if(count != -1){
			if(buffer[i] == '\n'){
				buffer[i+1] = '\0';

				if(tasks_running < MAXCOM){
					run_task(strdup(cenas));
					break;
				}
				else{
					push_waiting(strdup(buffer));
				}

				i = 0;
			}
			else i++;
		}
		else{
			write(STDOUT, "Error: error on input read\n", 27);
			free(buffer);
			return 1;
		}

	}
	//printf("MAP DONE!\n");
	
	// reduce

	int status;
	while(tasks_running > 0 || tasks_waiting > 0){ 
		//wait(&status);
		//printf("running: %d | waiting: %d\n", tasks_running, tasks_waiting);
	}
	//printf("Starting REDUCE\n");
	reduce(argv[2]);
	//printf("REDUCE DONE\n");






	return 0;
}