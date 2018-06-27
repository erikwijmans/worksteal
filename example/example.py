import sys
sys.path = ['..'] + sys.path
from worksteal import WorkStealingScheduler
import multiprocessing as mp
import time


def _worker_fn(work_queue, rank, output_queue):
    while True:
        task = work_queue.get_next_task()
        if task is None:
            break

        print('I am {}, I have {}, payload: {}, sleep: {}'.format(rank, *task))
        time.sleep(task[2])

        output_queue.put(None)


def main():
    mp_ctx = mp.get_context('forkserver')

    tasks = [
        (j, (j, 'hi {}'.format(j), i + 1)) for i in range(2) for j in range(2)
    ] + [(11, (11, 'final', 1)) for _ in range(10)]

    scheduler = WorkStealingScheduler(mp_ctx=mp_ctx)
    for t in reversed(tasks):
        scheduler.add_task(*t)

    output_queue = mp_ctx.Queue()
    workers = [
        mp_ctx.Process(
            target=_worker_fn,
            args=(scheduler.add_worker(), rank, output_queue)
        ) for rank in range(2)
    ]
    for w in workers:
        w.deamon = True
        w.start()

    scheduler.launch_scheduler()

    for _ in range(len(tasks)):
        _ = output_queue.get()

    tasks = [
        (j, (j, 'hi {}'.format(j), i + 1)) for i in range(2) for j in range(2)
    ] + [(11, (11, 'final', 1)) for _ in range(10)]

    scheduler.add_event.set()
    for t in tasks:
        scheduler.add_task(*t)

    scheduler.add_event.clear()

    for _ in range(len(tasks)):
        _ = output_queue.get()

    scheduler.close()

    for w in workers:
        w.join()


if __name__ == "__main__":
    main()
