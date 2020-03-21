import heapq


class HeapManager(object):
    def __init__(self):
        super(HeapManager, self).__init__()
        self.data = []

    def sync(self, items):
        if self.data:
            self.data[:] = []
        self.data = [(item[0], item) for item in items]
        heapq.heapify(self.data)

    def pop(self):
        return heapq.heappop(self.data)[1] if self.data else None

    def push(self, item):
        heapq.heappush(self.data, (item[0], item))

    def length(self):
        return len(self.data)
