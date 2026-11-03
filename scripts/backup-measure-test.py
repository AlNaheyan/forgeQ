#!/usr/bin/env python3
import importlib.util
import pathlib
import unittest
spec = importlib.util.spec_from_file_location('measure', pathlib.Path(__file__).with_name('backup-measure.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
class MeasurementTests(unittest.TestCase):
    def test_gate_and_chronology(self):
        fixture = {'snapshot':'2026-01-01T00:00:00Z','revision':'synthetic','size':100}
        times = [m.timestamp('2026-01-01T00:00:'+s+'Z') for s in ('10','15','20')]
        self.assertEqual(m.report(fixture,*times,5,(15,15),False,True)['target_gate'],'pending')
        self.assertEqual(m.report(fixture,*times,5,(15,15),True,True)['target_gate'],'passed')
        self.assertEqual(m.report(fixture,*times,5,(5,15),True,True)['target_gate'],'failed')
        self.assertEqual(m.report(fixture,*times,5,(None,None),True,True)['target_gate'],'pending')
        with self.assertRaises(ValueError): m.report(fixture,times[2],times[0],times[1],5,(15,15),True,True)
if __name__ == '__main__': unittest.main()
