/*
 * wait_stressor.c - Synthetic workload to exercise wait()/waitpid() paths.
 *
 * Overview
 * --------
 * 1) Main process forks PROCESS_COUNT children. Each child runs
 *    stressWaitSystemCall() and the main process waits for all children.
 *
 * 2) stressWaitSystemCall() (child process context) spawns two processes:
 *    - runnerProcess: loops on pause(), responding only to signals.
 *    - killerProcess: tight loop sending SIGSTOP/SIGCONT to the runner,
 *      causing rapid transitions between stopped and running states.
 *    The parent then continuously calls waitpid(..., WUNTRACED|WCONTINUED)
 *
 * 3) runnerProcess: simply pauses forever, receives SIGSTOP/SIGCONT.
 *
 * 4) killerProcess: repeatedly sends SIGSTOP then SIGCONT to the runner to
 *    generate a high volume of events observable via waitpid().
 *
 * Notes
 * -----
 * - This program intentionally contains infinite loops in the spawned
 *   processes to keep generating events until the test we terminate it.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>  
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

#define PROCESS_COUNT   52         // Number of children spawned by main()

#define UNUSED(x) (void)(x)

/*
 * spawnNewProcess
 * ----------------
 * Forks and runs the provided function in the child, passing pid_arg.
 * Returns child's PID to the parent; child never returns from this function.
 */
pid_t spawnNewProcess(void (*func)(const pid_t pid), const pid_t pid_arg) {
	pid_t pid = fork();
	if (pid == 0) {
		func(pid_arg);
		_exit(EXIT_SUCCESS);
	}
	return pid;
}

/*
 * killerProcess
 * --------------
 * Repeatedly toggles the target process between stopped and running states
 * by sending SIGSTOP followed by SIGCONT in a tight loop.
 */
void killerProcess(const pid_t pid) {
	pid_t parentPid = getppid();
	while (1) {
		kill(pid, SIGSTOP);
		sleep(0);
		kill(pid, SIGCONT);
	}

	printf("Killer process with PID: %d is sending SIGALRM to its parent\n", getpid());
	kill(getppid(), SIGALRM);
	_exit(EXIT_SUCCESS);
}

/*
 * runnerProcess
 * --------------
 * Idles by calling pause() forever. Its sole purpose is to receive signals
 * from killerProcess so that the parent has state transitions to observe.
 */
void runnerProcess(const pid_t pid) {
	UNUSED(pid);
	while (1) {
		pause();
	}
	printf("Runner process with PID: %d is sending SIGALRM to its parent\n", getpid());
	kill(getppid(), SIGALRM);
	_exit(EXIT_SUCCESS);
}

/*
 * WaitEvents
 * ---------------
 * Inner loop that continuously issues waitpid()/wait() to observe runner
 * state changes and to reap children when they terminate.
 */
static void WaitEvents(const pid_t runnerPid, const int options) {
	pid_t waitReturn;
	do {
		int status;
		waitReturn = waitpid(runnerPid, &status, options);
		if ((waitReturn < 0) && (errno != EINTR) && (errno != ECHILD)) {
			break;
		}
		waitReturn = wait(&status);
		if ((waitReturn < 0) && (errno != EINTR) && (errno != ECHILD)) {
			break;
		}
	} while (1);
}

/*
 * stressWaitSystemCall
 * ---------------------
 * Spawns a runner and a killer, then continually alternates between
 * waitpid() (with WUNTRACED|WCONTINUED to catch state changes) and wait()
 * (to reap any child that exits). The loop runs indefinitely to maintain
 * load on wait-related kernel paths.
 */
int stressWaitSystemCall(void) {
	int ret = EXIT_SUCCESS;
	pid_t runnerPid, killerPid, waitReturn;
	int options = WUNTRACED | WCONTINUED;
	runnerPid = spawnNewProcess(runnerProcess, 0);
	if (runnerPid < 0) {
		fprintf(stderr, "Error spawning runner process: %s\n", strerror(errno));
		return EXIT_FAILURE;
	}
	killerPid = spawnNewProcess(killerProcess, runnerPid);
	if (killerPid < 0) {
		fprintf(stderr, "Error spawning killer process: %s\n", strerror(errno));
		ret = EXIT_FAILURE;
	}
	WaitEvents(runnerPid, options);
	return ret;
}

/*
 * main
 * -----
 * Spawns PROCESS_COUNT children, each running the stress routine, then
 * waits for all children to exit.
 */
int main(void) {
	for (int i = 0; i < PROCESS_COUNT; i++) {
		pid_t pid = fork();
		if (pid == -1) {
			perror("fork");
			exit(EXIT_FAILURE);
		} else if (pid == 0) {
			printf("Main process has spawned a child with PID: %d\n", getpid());
			stressWaitSystemCall();
			exit(EXIT_SUCCESS);
		}
	}
	for (int i = 0; i < PROCESS_COUNT; i++) {
		printf("Main process is waiting for all children to exit\n");
		wait(NULL);
	}
	printf("All child processes have exited\n");
	return 0;
}
