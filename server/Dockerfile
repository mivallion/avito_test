FROM python:3.8

EXPOSE 8000

ADD main.py \
    common.py /
RUN pip install pymongo \
                fastapi \
                uvicorn \
                requests
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
