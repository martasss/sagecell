#! /usr/bin/env python

r"""
Kernel Provider starts compute kernels and sends connection info to Dealer.
"""


import argparse
import os
import signal
import threading

import zmq


from log import provider_logger


class KernelForker(threading.Thread):
    
    def __init__(self):
        super(KernelForker, self).__init__ ()

    def run(self):
        logger.debug("KernelForker started")
        socket = zmq.Context.instance().socket(zmq.PAIR)
        socket.connect("inproc://forker")
        
        

logger = provider_logger.getChild(str(os.getpid()))

def signal_handler(signum, frame):
    logger.info("Received %s, shutting down...", signum)
    exit(0)

signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

parser = argparse.ArgumentParser(
    description="Launch a kernel provider for SageMathCell")
parser.add_argument("--address",
    help="address of the kernel dealer (defaults to $SSH_CLIENT)")
parser.add_argument("port", type=int,
    help="port of the kernel dealer")
parser.add_argument("dir",
    help="directory name for user files saved by kernels")
args = parser.parse_args()
    
context = zmq.Context.instance()
dealer = context.socket(zmq.DEALER)
dealer.setsockopt(zmq.IPV6, 1)
address = args.address or os.environ["SSH_CLIENT"].split()[0]
if ":" in address:
    address = "[{}]".format(address)
address = "tcp://{}:{}".format(address, args.port)
logger.debug("connecting to %s", address)
dealer.connect(address)
logger.debug("connected to %s", address)
dealer.send("recommended settings")
if not dealer.poll(5000):
    logger.debug("dealer does not answer, terminating")
    exit(1)
settings = dealer.recv_json()
logger.debug("received %s", settings)

forker = context.socket(zmq.PAIR)
forker.bind("inproc://forker")
forker_thread = KernelForker()
forker_thread.start()

dealer.send("ready")

while True:
    msg = dealer.recv_json()
    logger.debug("received %s", msg)
    if msg == "stop":
        forker_thread.join()
        logger.debug("forker ended")
        exit(0)
    if msg[0] == "get":
        logger.debug("sending resource_limits back")
        dealer.send("kernel", zmq.SNDMORE)
        kernel = {
            "limits": msg[1],
            "id": "1234",
            "connection": {},
            }
        dealer.send_json(kernel)
    
