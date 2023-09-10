FROM python:3-slim
WORKDIR /app
COPY --chown=1000:1000 . ./
WORKDIR occ-daily-volume
RUN pip install --no-cache-dir -Ur requirements.txt
ENTRYPOINT ["python3", "volume-top-n.py"]
