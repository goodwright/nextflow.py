FROM sphinxdoc/sphinx as builder

RUN apt-get clean all && apt-get update && apt-get dist-upgrade -y && apt-get upgrade
RUN apt-get install --reinstall ca-certificates
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get install -y --no-install-recommends openjdk-11-jre
RUN apt-get install curl -y
RUN curl -L https://github.com/nextflow-io/nextflow/releases/download/v21.10.6/nextflow-21.10.6-all -o nextflow
RUN chmod +x nextflow
RUN mv nextflow /usr/local/bin

COPY ./ ./
RUN rm -rf docs/build

RUN pip install sphinx_rtd_theme

RUN cd docs && make html

FROM nginx:alpine
COPY --from=builder /docs/docs/build/html /usr/share/nginx/html