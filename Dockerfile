FROM public.ecr.aws/lambda/python:3.13

COPY uploader ${LAMBDA_TASK_ROOT}/uploader
COPY doc ${LAMBDA_TASK_ROOT}/doc

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

CMD ["set-me-in-serverless.yaml"]
