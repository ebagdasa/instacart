import time



class Timer:
    def __init__(self):
        self.time = dict()
        return

    def a(self, name='0'):
        self.time[name] = time.time()

    def b(self, name='0'):
        print(time.time() - self.time[name])


timer = Timer()