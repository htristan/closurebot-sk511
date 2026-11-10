# Use the official AWS Lambda Python 3.11 base image
FROM public.ecr.aws/lambda/python:3.11

# Install git and build tools (required for git dependencies and numpy compilation)
RUN yum update -y && yum install -y git gcc gcc-c++ make

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy application code
COPY scrape.py ${LAMBDA_TASK_ROOT}
COPY config.json ${LAMBDA_TASK_ROOT}

# Add build argument to differentiate images
ARG BUILD_TYPE=commit
RUN echo "Build type: $BUILD_TYPE" > /tmp/build_type.txt

# Set the CMD to your handler
CMD ["scrape.lambda_handler"]
