#include <mpi.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#define  WORKS_SIZE 20
#define COUNT_THREAD 2

int* works;
int* localWorks;
int counter = 0;
int localWorkCounter;
int refusalCounter = 0;

pthread_mutex_t sharedWorkMutex = PTHREAD_MUTEX_INITIALIZER;

void* Getter(void* arg) {
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    while (refusalCounter < size - 1) {
        int wantWork = -1;
        MPI_Status status;
        MPI_Recv(&wantWork, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
        int sendRank = status.MPI_SOURCE;
        if (counter < localWorkCounter - 1) {
            pthread_mutex_lock(&sharedWorkMutex);
            int workSize = localWorkCounter - 1 - counter;
            int sharedWork = workSize / 2 + 1;
            MPI_Send(&sharedWork, 1, MPI_INT, sendRank, 0, MPI_COMM_WORLD);
            int* startShared = localWorks + localWorkCounter - sharedWork;
            MPI_Send(startShared, sharedWork, MPI_INT, sendRank, 0, MPI_COMM_WORLD);
            localWorkCounter -= sharedWork;
            pthread_mutex_unlock(&sharedWorkMutex);
        }
        else {
            wantWork = -1;
            MPI_Send(&wantWork, 1, MPI_INT, sendRank, 0, MPI_COMM_WORLD);
            refusalCounter++;
        }
    }
    int  rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    printf("GETTER with rank %d CLOSED \n", rank);
    return NULL;
}

void GetWork(int* visited) {
    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    for (int i = 0; i < size; ++i) {
        if (visited[i] != 1) {
            MPI_Send(&rank, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            int countNewWork;
            MPI_Recv(&countNewWork, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (countNewWork == -1) {
                printf("NOT WORK for %d  form %d \n", rank, i);
                visited[i] = 1;
                continue;
            }
            printf("WORK form %d to %d count %d\n", i, rank, countNewWork );
            MPI_Recv(localWorks, countNewWork, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            localWorkCounter = countNewWork;
            break;
        }
    }
}

void* Work(void* arg) {
    int  rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int* visited = (int*)malloc(size * sizeof(int));
    visited[rank] = 1;
    while (localWorkCounter > 0) {
        while (counter < localWorkCounter) {
            printf("process with rank %d doing: %d  work %d for %d\n", rank, localWorks[counter], counter + 1, localWorkCounter );
            sleep(localWorks[counter]);
            pthread_mutex_lock(&sharedWorkMutex);
            counter++;
            pthread_mutex_unlock(&sharedWorkMutex);
        }
        localWorkCounter = -1;
        counter = 0;
        printf("WORKER with rank %d COMLETED all work \n", rank);
        GetWork(visited);
    }
    printf("WORKER with rank %d CLOSED \n", rank);
    free(visited);
    return NULL;
}

void InitWorks(int* works) {
    srand(time(NULL));
    for (int i = 0; i < WORKS_SIZE; ++i) {
        //works[i] = rand() % 10 + 1;
        works[i] = i + 1;
    }
}

int GetCount(int num, int count, int size) {
    int startIndex = size * num / count;
    int endIndex = size * (num + 1) / count;
    int countRow = endIndex - startIndex;
    return countRow;
}

void SharedWork() {
    printf("SHARING\n");
    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int* counts = (int*)malloc(size * sizeof(int));
    for (int i = 0; i < size; ++i) {
        counts[i] = GetCount(i, size, WORKS_SIZE);
    }
    int* shift = (int*)malloc(size * sizeof(int));
    shift[0] = 0;
    for (int i = 1; i < size; ++i) {
        shift[i] = shift[i - 1] + counts[i - 1];
    }
    MPI_Scatterv(works, counts, shift, MPI_INT, localWorks, counts[rank], MPI_INT, 0, MPI_COMM_WORLD);
    free(shift);
    free(counts);
}

int main(int argc, char* argv[]) {
    int rank, size, provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    double start_time, end_time;
    start_time = MPI_Wtime();
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    works = NULL;
    localWorks = NULL;
    counter = 0;
    if (rank == 0) {
        works = (int*)malloc(WORKS_SIZE * sizeof(int));
        InitWorks(works);
    }
    localWorkCounter = GetCount(rank, size, WORKS_SIZE);
    localWorks = (int*)malloc(localWorkCounter * sizeof(int));
    MPI_Barrier(MPI_COMM_WORLD);
    SharedWork();
    pthread_attr_t attrs;
    if (pthread_attr_init(&attrs) != 0) {
        perror("Cannot initialize attributes");
        abort();
    }
    if (pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE) != 0) {
        perror("Error in setting attributes");
        abort();
    }
    pthread_t* thrs = (pthread_t*)malloc(COUNT_THREAD * sizeof(pthread_t));
    if (pthread_create(&thrs[0], &attrs, Work, NULL) != 0) {
        perror("Cannot create a thread");
        abort();
    }
    if (pthread_create(&thrs[1], &attrs, Getter, NULL) != 0) {
        perror("Cannot create a thread");
        abort();
    }
    pthread_attr_destroy(&attrs);
    for (int i = 0; i < COUNT_THREAD; i++) {
        if (pthread_join(thrs[i], NULL) != 0) {
            perror("Cannot join a thread");
            abort();
        }
    }
    printf("PROC %d closed\n", rank);
    free(works);
    free(localWorks);
    end_time = MPI_Wtime();
    printf("Total time: %f seconds\n", end_time - start_time);
    MPI_Finalize();
    return 0;
}