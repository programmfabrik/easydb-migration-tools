class BatchMode:
    List = 'List'
    Dictionary = 'Dictionary'

class BatchedJob(object):
    def __init__(self, mode, batch_size, function, *args, **kwargs):
        self.mode = mode
        self.batch_size = batch_size
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.batch_nr = 1
        self._reset()
    def add(self, element, key=None):
        if self.mode == BatchMode.List:
            self.batch.append(element)
        elif key is None:
            raise Exception('BatchedJob.add for BatchMode.Dictionary requires key')
        else:
            self.batch[key] = element
        if (len(self.batch) >= self.batch_size):
            self._do()
            self.batch_nr += 1
    def finish(self):
        if (len(self.batch) > 0):
            self._do()
    def _do(self):
        self.function(self.batch, *self.args, **self.kwargs)
        self._reset()
    def _reset(self):
        if self.mode == BatchMode.List:
            self.batch = []
        else:
            self.batch = {}
