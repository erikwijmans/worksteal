import multiprocessing as mp
from .task_queue import TaskQueue
import queue
import time


class WorkStealingScheduler(object):
    r"""Implements a working stealing scheduler

    The scheduler is a static scheduler. It must know about ALL tasks and workers before
    begining the schduler

    Parameters
    ----------
    duplication_ratio: int
        The ratio between the expected time to duplicate the resource needed a task
        and the expected time to complete the task.  This is used to decided whether to steal or not.
        If we cannot leave at least 2*duplication_ratio in the task queue, we do not steal as we assume
        that all the work would be done at the same rate and that stealing/duplication is always
        less preferable.
    """

    def __init__(self, duplication_ratio: int = 1):
        self.mp_ctx = mp.get_context()

        self.request_queue = self.mp_ctx.Queue()
        self.done_event = self.mp_ctx.Event()
        self.add_event = self.mp_ctx.Event()
        self.done_event.clear()
        self.add_event.clear()

        self.resource_queue = []
        self.task_by_resource = {}
        self.task_by_worker = {}
        self.resource_by_worker = {}
        self.response_queue_by_worker = {}
        self.outstanding_request_queue = []

        self.sched = None
        self.add_queue = None
        self.steal_threshold = 2 * duplication_ratio

    def add_worker(self):
        _id = len(self.task_by_worker)
        self.task_by_worker[_id] = []
        self.resource_by_worker[_id] = None

        self.response_queue_by_worker[_id] = self.mp_ctx.Queue()

        return TaskQueue(
            _id, self.request_queue, self.response_queue_by_worker[_id]
        )

    def add_task(self, resource_id, task):
        if self.sched is None:
            if resource_id in self.task_by_resource:
                self.task_by_resource[resource_id].append(task)
            else:
                self.resource_queue.append(resource_id)
                self.task_by_resource[resource_id] = [task]
        else:
            self.add_queue.put((resource_id, task))

    def _attempt_steal(self, _id):
        # Attempt to steal from a worker who has the same resource we do
        to_steal_id = None
        largest_undone_amount = 0
        for k, v in self.resource_by_worker.items():
            if v == self.resource_by_worker[_id]:
                if len(self.task_by_worker[k]) > largest_undone_amount:
                    largest_undone_amount = len(self.task_by_worker[k])
                    to_steal_id = k

        # Steal half the work if possible.  Otherwise leave steal_threshold in the queue
        steal_amount = min(
            largest_undone_amount // 2,
            largest_undone_amount - self.steal_threshold
        )

        # We can't steal from them :( So lets steal from someone else!
        if steal_amount <= 0:
            to_steal_id = None
            largest_undone_amount = 0

            # Find the worker with the most amount of undone work to steal from
            for k, v in self.task_by_worker.items():
                if len(v) > largest_undone_amount:
                    largest_undone_amount = len(v)
                    to_steal_id = k

            # Steal half the work if possible.  Otherwise leave steal_threshold in the queue
            steal_amount = min(
                largest_undone_amount // 2,
                largest_undone_amount - self.steal_threshold
            )

        # If there is not enough work to steal, we simply return
        if steal_amount <= 0:
            return

        steal_queue = self.task_by_worker[to_steal_id]
        self.task_by_worker[to_steal_id] = steal_queue[steal_amount:]
        self.task_by_worker[_id] = steal_queue[0:steal_amount]
        self.resource_by_worker[_id] = self.resource_by_worker[to_steal_id]

    def _pop_task_queue(self, _id):
        if len(self.task_by_worker[_id]) > 0:
            task = self.task_by_worker[_id].pop()
            return task
        else:
            return None

    def _sched_loop(self):
        while True:
            if self.done_event.is_set():
                for k, v in self.response_queue_by_worker.items():
                    print('Sending None to worker_id: {}'.format(k))
                    v.put(None)
                break

            if self.add_event.is_set() or not self.add_queue.empty():
                try:
                    resource_id, task = self.add_queue.get(timeout=1)

                    found_worker = False
                    for k, v in self.resource_by_worker.items():
                        if v == resource_id:
                            self.task_by_worker[k].append(task)
                            found_worker = True
                            break

                    if not found_worker:
                        if resource_id in self.task_by_resource:
                            self.task_by_resource[resource_id].append(task)
                        else:
                            self.resource_queue.append(resource_id)
                            self.task_by_resource[resource_id] = [task]

                    while len(self.outstanding_request_queue) > 0:
                        self.request_queue.put(
                            self.outstanding_request_queue.pop()
                        )
                except queue.Empty:
                    pass
            else:
                try:
                    _id = self.request_queue.get(timeout=1)

                    # This worker has nothing left to do!
                    if len(self.task_by_worker[_id]) == 0:
                        # Assign them the next resource in the queue, if there is new work for the resource
                        # they already have, give them that!
                        if len(self.resource_queue) > 0:
                            if (self.resource_by_worker[_id] in
                                    self.resource_queue):
                                resource = self.resource_by_worker[_id]
                                self.resource_queue.remove(resource)
                            else:
                                resource = self.resource_queue.pop()

                            self.task_by_worker[_id] = (
                                self.task_by_resource.pop(resource)
                            )
                            self.resource_by_worker[_id] = resource
                        # There are no more unassigned resources, so we need to steal work!
                        else:
                            self._attempt_steal(_id)

                    task = self._pop_task_queue(_id)
                    if task is not None:
                        self.response_queue_by_worker[_id].put(task)
                    else:
                        self.outstanding_request_queue.append(_id)
                except queue.Empty:
                    pass

    def launch_scheduler(self):
        assert self.sched is None

        self.add_queue = self.mp_ctx.Queue()
        self.sched = self.mp_ctx.Process(target=self._sched_loop, args=())
        self.sched.deamon = True
        self.sched.start()

    def close(self):
        if self.sched is not None:
            self.done_event.set()

            self.sched.join()
            self.sched = None

    def __del__(self):
        self.close()
