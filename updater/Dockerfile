FROM python:3.8
ADD main.py \
    common.py /
RUN pip install pymongo \
                requests
CMD ["python", "./main.py"]
