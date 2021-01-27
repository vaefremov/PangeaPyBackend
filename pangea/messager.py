#!/opt/PANGmisc/bin/python4
# -*- coding: utf-8 -*-
"""This module implements procedures to
output messages for ReView application server.
"""

__version__ = "$Revision: 3702 $"[11:-2]  # code version

import sys

#############################################################
##  Messages
#############################################################

class Messager:
    """Writes messages about stage of process, current step
    and gauge value to stderr.
    """
    def __init__(self):
        self.gauge = 0

    def setGauge(self, v, max_value=100.0, min_value=0.0):
        """Set gauge value and output message
        """
        value = ((v - min_value)/(max_value - min_value)) * 100.
        value = int(value)
        if value > 100:
            value = 100
        elif value < 0:
            value = 0
        if value == self.gauge:
            return
        print('Messager::SetGauge = %d' % value, file=sys.stderr, flush=True)
        self.gauge = value

    def setStep(self, stepName):
        """Output message about the stage (step) of job
        """
        print('Messager: CurMessage = %s' % stepName, file=sys.stderr, flush=True)
        print('Messager::SetGauge = 0', file=sys.stderr, flush=True)

if __name__ == "__main__":
    # testing
    m = Messager()

    m.setStep("step 0")
    for i in range(0, 101):
        m.setGauge(i)

    m.setStep("step 1")
    for i in range(-12, 120, 10):
        m.setGauge(i)
        
    m.setStep("step 2")
    vmax = 31.3
    for i in range(0, 300):
        v = (i * 0.12) / vmax
        m.setGauge(v)

    m.setStep("step 3")
    vmax = 31.3
    for i in range(0, 300):
        v = (i * 0.12) / vmax
        m.setGauge(v, vmax)
