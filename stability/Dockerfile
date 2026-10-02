ARG BASE_TEST_IMAGE=arcus-python-stability-tests
FROM ${BASE_TEST_IMAGE}
WORKDIR /stability
COPY stability/runner.py ./runner.py
ENTRYPOINT ["timeout", "--signal=TERM", "--kill-after=10s", "1800s"]
CMD ["python", "-u", "runner.py"]
