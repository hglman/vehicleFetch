FROM python:3

COPY src/ .
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["gunicorn", "main:app", "-b", "0.0.0.0:8100"]
EXPOSE 8001