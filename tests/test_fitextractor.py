
import unittest

import fitdecode

from fitextractor import *


class TestFitExtractor(unittest.TestCase):

    def setUp(self):
        self.files = ['tests/test_files/f1.fit']

    def test_init(self):
        fe = FitExtractor(self.files[0])

    def test_parsing(self):
        fe = FitExtractor(self.files[0])
        messages = fe.messages

        self.assertTrue(isinstance(fe.header, fitdecode.records.FitHeader))
        self.assertTrue(isinstance(fe.crc, fitdecode.records.FitCRC))
        self.assertTrue(len(messages.keys()) > 0)