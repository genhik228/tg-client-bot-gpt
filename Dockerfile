FROM python:3.10-bullseye

WORKDIR /src

COPY ./tg-client-bot-gpt .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python3.10", "main.py" ]