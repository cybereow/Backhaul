#!/usr/bin/env python3
# ponytail: throwaway SFTP put helper. Delete after. Creds from env.
import os, sys, paramiko
host, user, pw = os.environ["RHOST"], os.environ["RUSER"], os.environ["RPASS"]
local, remote = sys.argv[1], sys.argv[2]
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=pw, timeout=20, look_for_keys=False, allow_agent=False)
sf = c.open_sftp()
sf.put(local, remote)
print(f"put {local} -> {host}:{remote} ({sf.stat(remote).st_size} bytes)")
sf.close(); c.close()
