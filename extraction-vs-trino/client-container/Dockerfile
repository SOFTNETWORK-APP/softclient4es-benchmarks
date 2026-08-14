# Benchmark client, containerised so a memory cap is REAL.
#
# Why not rlimit: RLIMIT_AS is unusable on macOS -- `setrlimit` is rejected
# outright ("current limit exceeds maximum limit"), and where it is accepted it
# does not bite: a probe allocated 900 MB under a 600 MB cap. A cap that
# silently does nothing would produce a "both engines completed" result that
# means nothing, which is the worst possible outcome for this scenario.
#
# A Docker --memory limit is a real cgroup bound inside the Linux VM, enforced
# by the kernel OOM killer, and it is also the shape of the actual claim:
# "does this fit in a 4 GB container?"
FROM python:3.12-slim

# Same pins as requirements.txt -- the client versions are load-bearing for
# comparability (arrow#152), so they must not drift between host and container.
RUN pip install --no-cache-dir \
      pyarrow==25.0.0 \
      adbc_driver_manager==1.12.0 \
      adbc_driver_flightsql==1.12.0 \
      pandas==3.0.5 \
      trino==0.338.0 \
      sqlalchemy==2.0.51 \
      connectorx==0.4.5

WORKDIR /bench
COPY run_capped.py /bench/run_capped.py
ENTRYPOINT ["python", "/bench/run_capped.py"]
