from google_auth_oauthlib.flow import InstalledAppFlow
import pickle

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://mail.google.com/'
]

flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
creds = flow.run_local_server(port=8080)

with open('new_token.pickle', 'wb') as f:
    pickle.dump(creds, f)

print("✅ SUCCESS")
print("New Refresh Token:", creds.refresh_token)
print("Client ID used:", creds.client_id)
print("All scopes:", creds.scopes)
