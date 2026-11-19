FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY fake_upstream ./fake_upstream
COPY run_service.py ./run_service.py

# NTP/udp 123 and HTTP 8000. The service needs no root: the container is
# granted CAP_NET_BIND_SERVICE for the privileged NTP port.
RUN useradd --system --uid 10001 --home /srv ntp && \
    mkdir -p /data && chown ntp:ntp /data
USER ntp

EXPOSE 123/udp
EXPOSE 8000/tcp

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"

CMD ["python", "run_service.py"]
