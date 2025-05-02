# write a python dockerfile
FROM python:3.9

# set the working directory
WORKDIR /app
# copy the requirements file
COPY requirements.txt .
# install the requirements
RUN pip install -r requirements.txt
# copy the rest of the files
COPY src/ .
# run the app
CMD ["python","-u", "consumer.py"]