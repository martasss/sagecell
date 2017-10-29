#! /usr/bin/env python

import json
import psutil
import shlex
import subprocess
import time
import urllib
import urllib2


lasterror = start = time.time()
subprocess.Popen(shlex.split("../sage/sage web_server.py"))

while True:
    try:
        data = urllib.urlencode(dict(code="print(1+2)", accepted_tos="true"))
        request = urllib2.urlopen("http://localhost:8888/service", data)
        reply = json.loads(request.read())
        print(reply)
        if reply["success"] and "stdout" in reply:
            print("last error was at {}".format(lasterror - start))
            print("correct reply obtained at {}".format(time.time() - start))
            request = urllib2.urlopen("http://localhost:8888/service", data)
            reply = json.loads(request.read())
            print(reply)
            print("second correct reply obtained at {}".format(time.time() - start))
            break
    except Exception as e:
        lasterror = time.time()
    time.sleep(0.1)

time.sleep(1)   # Let non-participating code work a bit as well
for p in psutil.process_iter(attrs=["pid", "cmdline"]):
    if p.info["cmdline"] == ["python", "web_server.py"]:
        p.terminate()
print("shutdown at {}".format(time.time() - start))
