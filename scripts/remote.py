#!/usr/bin/env python3
# ponytail: throwaway remote-exec helper for real-infra probe test. Delete after.
# Creds come from env (RHOST/RUSER/RPASS) so they stay out of argv/history.
import os, sys, paramiko

host = os.environ["RHOST"]
user = os.environ["RUSER"]
pw   = os.environ["RPASS"]

c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=pw, timeout=20, look_for_keys=False, allow_agent=False)

cmd = sys.argv[1]
stdin, stdout, stderr = c.exec_command(cmd, timeout=int(os.environ.get("RTIMEOUT", "60")))
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
rc  = stdout.channel.recv_exit_status()
sys.stdout.write(out)
if err.strip():
    sys.stderr.write("--- stderr ---\n" + err)
c.close()
sys.exit(rc)
