import time
import unittest
from parallel import Parallel

# simulate network calls using dummy network
class DummyNetwork:
    def __init__(self, counter=0):
        self.counter = counter

    def call(self):
        time.sleep(0.1)
        return DummyNetwork(counter=self.counter + 1)

    def get_counter(self):
        time.sleep(0.1)
        return self.counter
    
    def returnint(self, i):
        time.sleep(0.1)
        return i

class TestAutoParallel(unittest.TestCase):
    def setUp(self):
        self.para = Parallel(serial=False)
    
    def test_basic(self):
        start_time = time.time()
        dummy_network = DummyNetwork()
        for i in range(200):
            self.para.dummy_network.call()
        self.para.execute()
        end_time = time.time()
        # serial would take 20 seconds, parallel should take < 0.3 seconds
        self.assertLessEqual(end_time - start_time, 0.3)

    def test_auto(self):
        @self.para.auto
        def foo():
            dummy_network = DummyNetwork()
            data_numbers = range(100)
            # TODO: make range(100) work, it doesn't work right now because
            # para.range(100) returns None
            for return_data in self.para.data_numbers:
                output = self.para.dummy_network.returnint(return_data)
                self.assertEqual(output, return_data)
        start_time = time.time()
        foo()
        end_time = time.time()
        self.assertLessEqual(end_time - start_time, 0.2)

if __name__ == '__main__':
    unittest.main()