import os.path

# from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth
import tempfile

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

path_to_token = '/Users/guylitt/git/fsds/scripts/config/token.json'





if os.path.exists(path_to_token):
    creds = Credentials.from_authorized_user_file(path_to_token, SCOPES)

try:
    service = build("drive", "v3", credentials=creds)

    # Call the Drive v3 API
    results = (
        service.files()
        .list(pageSize=3, fields="nextPageToken, files(id, name)")
        .execute()
    )
    items = results.get("files", [])

    if not items:
        print("No files found.")
        #return
    print("Files:")
    for item in items: 
        print(f"{item['name']} ({item['id']})")
except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")

# Requirement for GoogleAuth(): Change to the path where client_secrets.json lives!!
os.chdir(os.path.dirname(path_to_token)) 
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
#gauth.LoadCredentialsFile("/Users/guylitt/git/fsds/scripts/config/creds.json")#LoadClientConfig()#LoadClientConfigFile(path_to_token)#.LoadCredentialsFile(path_to_token)
drive = GoogleDrive(gauth)


home_dir = tempfile.TemporaryDirectory()
dir_save= f'{home_dir}/noaa/regionalization/data/input'

os.makedirs(dir_save, exist_ok = True)

path_data = f'{home_dir}/noaa/regionalization/data/julemai-xSSA/data_in/basin_metadata/basin_validation_results.txt'
name_data = os.path.basename(path_data)


query = {'q': f"title = '{name_data}'"}
files = drive.ListFile(query).GetList()

saved_files = list()
for file_list in drive.ListFile(query):
  print('Received %s files from Files.list()' % len(file_list)) # <= 10
  for file1 in file_list:
      print('title: %s, id: %s' % (file1['title'], file1['id']))
      path_save_file = f"{dir_save}/{file1['title']}"
      saved_files.append(path_save_file)
      file1.GetContentFile(path_save_file) # Works!!
      print(f'Wrote file to: {path_save_file}')


