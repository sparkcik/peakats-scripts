FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install flask gunicorn sqlalchemy psycopg2-binary google-generativeai google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client pandas python-dotenv requests --no-cache-dir
EXPOSE 8080
CMD ["python3", "infra/forge_runner.py"]
