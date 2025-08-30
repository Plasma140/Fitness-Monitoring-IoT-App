FROM python:3.9

WORKDIR /app

COPY subscriber_processor.py /app
COPY requirements.txt /app
COPY training_data.csv /app
COPY app.py /app
COPY caMQTT.crt /app
COPY processor.crt /app
COPY processor.key /app

RUN pip install -r requirements.txt

COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 5001
CMD ["/start.sh"]
#CMD ["python", "-u", "/app/subscriber_processor.py"]
#CMD ["ls", "-l", "/app"]