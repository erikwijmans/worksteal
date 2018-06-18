from worksteal import WorkStealingScheduler
import multiprocessing as mp
import time


def _worker_fn(work_queue, rank):
    while True:
        task = work_queue.get_next_task()
        if task is None:
            break

        print('I am {}, I have {}, payload: {}, sleep: {}'.format(rank, *task))
        time.sleep(task[2])


def main():
    tasks = [
        (j, (j, 'hi {}'.format(j), i + 1)) for i in range(5) for j in range(5)
    ] + [(11, (11, 'final', 2)) for _ in range(10)]

    scheduler = WorkStealingScheduler()
    for t in reversed(tasks):
        scheduler.add_task(*t)

    workers = [
        mp.Process(target=_worker_fn, args=(scheduler.add_worker(), rank))
        for rank in range(5)
    ]
    for w in workers:
        w.deamon = True
        w.start()

    scheduler.launch_scheduler()
    for w in workers:
        w.join()

    scheduler.close()


if __name__ == "__main__":
    main()
