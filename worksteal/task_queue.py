import multiprocessing as mp


class TaskQueue(object):

    def __init__(self, _id, request_queue, response_queue):
        self._id = _id
        self.request_queue = request_queue
        self.response_queue = response_queue

    def get_next_task(self):
        self.request_queue.put(self._id)

        return self.response_queue.get()
