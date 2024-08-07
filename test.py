import time
import unittest
from parallel import Parallel

# simulate network calls using dummy network
class DummyNetwork:
    def __init__(self, data=0):
        self.data = data

    def call(self):
        time.sleep(0.1)

    def get_another_network_with_data(self, data: int):
        time.sleep(0.1)
        return DummyNetwork(data)
    
    def get_data(self):
        time.sleep(0.1)
        return self.data
    
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
                main_output = self.para.dummy_network.returnint(return_data)
                sub_network = self.para.dummy_network.get_another_network_with_data(return_data)
                sub_network_output = self.para.sub_network.get_data()
                self.assertEqual(main_output, return_data)
                self.assertEqual(sub_network_output, return_data)
        start_time = time.time()
        foo()
        end_time = time.time()
        self.assertLessEqual(end_time - start_time, 0.75)

if __name__ == '__main__':
    unittest.main()
