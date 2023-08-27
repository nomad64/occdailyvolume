FROM python:3-slim
WORKDIR /app
COPY . ./
WORKDIR occ-daily-volume
RUN pip install --no-cache-dir -Ur requirements.txt
ENTRYPOINT ["python3", "volume-top-n.py"]