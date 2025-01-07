FROM python:3

WORKDIR /app

COPY . .

RUN pip install boto3 python-dateutil

CMD [ "python",  "upar_csvs.py" ]