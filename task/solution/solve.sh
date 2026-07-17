#!/bin/bash
set -euo pipefail
cp /solution/mesh_supervisor.py /app/src/mesh_supervisor.py
chmod 0755 /app/src/mesh_supervisor.py /app/src/worker.py
make -C /app clean all
/usr/bin/python3 /solution/solve.py
