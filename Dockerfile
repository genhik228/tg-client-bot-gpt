FROM python:3.10-bullseye
WORKDIR /src
COPY ./tg-bot .
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3.10", "bot.py" ]