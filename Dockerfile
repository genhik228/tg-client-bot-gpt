FROM python:3.10-bullseye

WORKDIR /src

COPY ./tg-client-bot-gpt .

RUN pip install --user --cache-dir /pip-cache -r requirements.txt
COPY --from=builder /root/.local /root/.local
COPY --from=builder /pip-cache /pip-cache

ENV PATH=/root/.local/bin:$PATH

ENTRYPOINT [ "python3.10", "main.py" ]