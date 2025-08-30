FROM python:3.9

WORKDIR /app

COPY training.py /app
COPY requirements.txt /app
COPY model /app/model
COPY modules /app/modules

RUN pip install -r requirements.txt

EXPOSE 5000
CMD ["python", "-u","/app/training.py"]
#CMD ["ls", "-l", "/app"]
