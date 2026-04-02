FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install flask gunicorn sqlalchemy psycopg2-binary google-generativeai google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client pandas python-dotenv requests schedule twilio pytz --no-cache-dir
EXPOSE 8080
CMD ["sh", "-c", "cd /app/infra && gunicorn --bind 0.0.0.0:8080 --workers 2 --timeout 120 forge_runner:app"]
