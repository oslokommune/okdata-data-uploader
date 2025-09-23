FROM public.ecr.aws/lambda/python:3.13

COPY uploader ${LAMBDA_TASK_ROOT}/uploader
COPY doc ${LAMBDA_TASK_ROOT}/doc

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

RUN dnf install shadow-utils -y
RUN /sbin/groupadd -r app
RUN /sbin/useradd -r -g app app
RUN chown -R app:app ${LAMBDA_TASK_ROOT}
USER app

CMD ["set-me-in-serverless.yaml"]
