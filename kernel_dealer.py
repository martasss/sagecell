import paramiko
import time
import zmq


from log import logger
import misc


config = misc.Config()


class KernelDealer(object):
    r"""
    Kernel Dealer handles compute kernels on the server side.
    """
    
    def __init__(self, provider_settings):
        self.provider_settings = provider_settings
        self._available_providers = []
        self._connected_providers = set()
        self._expected_kernels = []
        self._get_queue = []
        self._kernels = dict()
        # id: {
        #   "connection": { ??????????????????????????????????????????
        #     "key": hmac_key,
        #     "hb_port": hb,
        #     "iopub_port": iopub,
        #     "shell_port": shell,
        #     "stdin_port": stdin,
        #   },
        #   "executing": integer,
        #   "referer": URI,
        #   "remote_ip": ip,
        #   "timeout": time,
        #   "hard_deadline": time,
        #   "deadline": time,
        # }        
        context = zmq.Context.instance()
        socket = context.socket(zmq.ROUTER)
        socket.setsockopt(zmq.IPV6, 1)
        self._port = socket.bind_to_random_port("tcp://*")
        # Can configure perhaps interface/IP/port
        self._stream = zmq.eventloop.zmqstream.ZMQStream(socket)
        self._stream.on_recv(self._recv)
        logger.debug("KernelDealer initialized")
        
    def _try_to_get(self):
        r"""
        Send a get request if possible AND needed.
        """
        while self._available_providers and self._get_queue:
            self._stream.send(self._available_providers.pop(0), zmq.SNDMORE)
            self._stream.send_json(["get", self._get_queue.pop(0)])
        if self._available_providers:
            logger.debug("%s available providers are idling",
                len(self._available_providers))
        if self._get_queue:
            logger.debug("%s get requests are waiting for providers",
                len(self._get_queue))
        
    def _recv(self, msg):
        logger.debug("received %s", msg)
        addr, msg = msg[0], msg[1:]
        self._connected_providers.add(addr)
        if msg == ["recommended settings"]:
            self._stream.send(addr, zmq.SNDMORE)
            self._stream.send_json(self.provider_settings)
        elif msg == ["ready"]:
            self._available_providers.append(addr)
            self._try_to_get()
        elif msg[0] == "kernel":
            msg = zmq.utils.jsonapi.loads(msg[1])
            for i, (limits, callback) in enumerate(self._expected_kernels):
                if limits == msg["limits"]:
                    self._expected_kernels.pop(i)
                    callback(msg)                    
            
    def get_kernel(self, resource_limits, referer, remote_ip, timeout, callback):

        def cb(reply):
            id = reply["id"]
            now = time.time()
            self._kernels[id] = kernel = {
                "connection": reply["connection"],
                "executing": 0,
                "referer": referer,
                "remote_ip": remote_ip,
                "timeout": min(timeout, config.get_config("max_timeout")),
                "hard_deadline": now + config.get_config("max_lifespan"),
                }
            #self._sessions[id] = Session(key=kernel["connection"]["key"])
            kernel["deadline"] = min(
                now + kernel["timeout"], kernel["hard_deadline"])
            logger.info("activated kernel %s", id)
            callback(id)
            
        self._expected_kernels.append((resource_limits, cb))
        self._get_queue.append(resource_limits)
        self._try_to_get()

    def start_providers(self, providers, dir):
        r"""
        Start kernel providers.
        
        INPUT:
        
        - ``configs`` -- list of dictionaries
        
        - ``dir`` -- directory name for user files saved by kernels
        """
        for config in providers:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(config["host"], username=config["username"])
            command = "{} '{}/kernel_provider.py' {} '{}'".format(
                config["python"], config["location"], self._port, dir)
            logger.debug("starting kernel provider: %s", command)
            client.exec_command(command)
            client.close()
            #session = client.get_transport().open_session()
            #session.exec_command(command)
            #self._started_providers.append(
            #    {"client": client, "session": session})
        
    def stop(self):
        r"""
        Notify all providers that we are shutting down.
        """
        self._stream.stop_on_recv()
        for addr in self._connected_providers:
            logger.debug("stopping %r", addr)
            self._stream.send(addr, zmq.SNDMORE)
            self._stream.send("stop")
        self._stream.flush()
