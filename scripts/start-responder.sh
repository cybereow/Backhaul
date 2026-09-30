#!/bin/bash
# ponytail: throwaway responder launcher for test box. Frees :53 from systemd-resolved.
set -e
sudo systemctl stop systemd-resolved 2>/dev/null || true
sudo pkill -f 'backhaul -probe' 2>/dev/null || true
sleep 1
sudo setsid bash -c '/home/ubuntu/backhaul -probe /home/ubuntu/probe.responder.toml > /home/ubuntu/responder.log 2>&1 < /dev/null' &
sleep 2
echo "==log=="
cat /home/ubuntu/responder.log
echo "==udp53=="; sudo ss -lunp | grep ':53 ' || true
echo "==tcp53=="; sudo ss -ltnp | grep ':53 ' || true
