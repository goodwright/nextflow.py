FROM sphinxdoc/sphinx as builder

RUN touch /usr/local/bin/nextflow && chmod +x /usr/local/bin/nextflow

COPY ./ ./
RUN rm -rf docs/build

RUN pip install sphinx_rtd_theme

RUN cd docs && make html

FROM nginx:alpine
COPY --from=builder /docs/docs/build/html /usr/share/nginx/html