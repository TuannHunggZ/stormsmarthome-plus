FROM python:3.13-slim

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY ./storm_exporter.py ./

EXPOSE 8082

CMD ["python","storm_exporter.py"]