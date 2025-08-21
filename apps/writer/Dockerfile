
FROM python:3.12 AS builder

WORKDIR /opt

RUN python -m venv venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

COPY src/writer.py .

RUN chown -R 1000:0 /app && \
    chown -R 1000:0 /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

USER 1000

EXPOSE 8080
# 10. Set the command to run your application
CMD ["python", "-u", "writer.py"]