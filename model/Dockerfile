FROM python:3.8-buster

WORKDIR /model

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY credentials ./credentials

COPY job .

CMD [""]
ENTRYPOINT ["python3", "main.py"]

