FROM python:3.7

ENV TZ Asia/Shanghai

RUN apt-get update -y \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
ADD requirements.txt /usr/src/app

RUN pip install --upgrade pip && pip install -r requirements.txt

ADD . /usr/src/app
WORKDIR /usr/src/app/skyhook
ENTRYPOINT ["python", "main.py"]
