FROM python:latest

LABEL Maintainer="mommalongnips"

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py ./

CMD [ "python", "-u", "./main.py"]
