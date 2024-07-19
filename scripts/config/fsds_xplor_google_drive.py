import os.path

# from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth
import tempfile

#%% googleapi approach
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

#%% The pydrive approach

# Requirement for GoogleAuth(): Change to the path where client_secrets.json lives!!
os.chdir(os.path.dirname(path_to_token)) 
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
#gauth.LoadCredentialsFile("/Users/guylitt/git/fsds/scripts/config/creds.json")#LoadClientConfig()#LoadClientConfigFile(path_to_token)#.LoadCredentialsFile(path_to_token)
drive = GoogleDrive(gauth)


# TODO add option for temp or user-specified.
home_dir = tempfile.TemporaryDirectory()
dir_save= f'{home_dir}/noaa/regionalization/data/input'

os.makedirs(dir_save, exist_ok = True)

# TODO acquire this from yaml config
path_data = f'{home_dir}/noaa/regionalization/data/julemai-xSSA/data_in/basin_metadata/basin_validation_results.txt'
name_data = os.path.basename(path_data)
#%% pydrive download
# Identify file(s) of interest 
query = {'q': f"title = '{name_data}'"}
files = drive.ListFile(query).GetList()
# Download file(s)
saved_files = list()
for file_list in drive.ListFile(query):
  print('Received %s files from Files.list()' % len(file_list)) # <= 10
  for file1 in file_list:
      print('title: %s, id: %s' % (file1['title'], file1['id']))
      path_save_file = f"{dir_save}/{file1['title']}"
      saved_files.append(path_save_file)
      file1.GetContentFile(path_save_file) # Works!!
      print(f'Wrote file to: {path_save_file}')

#%% List subdirectories of interest


titles_to_match = "RegionalizationCollab/FSDS/temp_data/input/user_data_std/".split('/')

def list_and_match_subfolders(parent_folder_id, titles_to_match, indent=0):
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folder_list = drive.ListFile({'q': query}).GetList()
    
    matches = []
    
    for folder in folder_list:
        if folder['title'] in titles_to_match:
            print(' ' * indent + f"Matched Title: {folder['title']}, ID: {folder['id']}")
            matches.append(folder)
        # Recursively list subfolders of the current folder and extend matches
        matches.extend(list_and_match_subfolders(folder['id'], titles_to_match, indent + 4))
    
    return matches

parent_folder_id = 'root'  # Replace with your parent folder ID
ls_id_all_sf = list_and_match_subfolders(parent_folder_id, titles_to_match)

if ls_id_all_sf[-1]['title'] != titles_to_match[-1]:
    raise ValueError(f"Could not find the final google drive folder '{titles_to_match[-1]}'. Only found up to '{ls_id_all_sf[-1]['title']}'")


#%% pydrive upload

# TODO create a folder: https://developers.google.com/drive/api/guides/folder#create_a_folder
# Create folder.
folder_metadata = {
    "title": "RegionalizationCollab/FSDS/temp_data/input/user_data_std/test_folder_pydrive2",
    # The mimetype defines this new file as a folder, so don't change this.
    "mimeType": "application/vnd.google-apps.folder",
}
folder = drive.CreateFile(folder_metadata)
folder.Upload()



# TODO upload file


# Identify folder:
# TODO reference folderName
folderName = 'user_data_std'  # Please set the folder name.

folders = drive.ListFile(
    {'q': "title='" + folderName + "' and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList()
for folder in folders:
    if folder['title'] == folderName:
        file2 = drive.CreateFile({'parents': [{'id': folder['id']}]})
        file2.SetContentFile('new_test.csv')
        file2.Upload()

#%% folder_i
# The sharing url for user_data_std
subdirid_user_data_std = 'https://drive.google.com/drive/folders/1kqIIEmVQvhSawEBXUzezzk-0IH-5awdk?usp=drive_link'

file1 = drive.CreateFile({'title': 'file.txt', 'parents': [{'id': subdirid_user_data_std}]})
file1.SetContentFile(path_save_file)
file1.Upload()

#%% Experimental from https://stackoverflow.com/questions/34101427/accessing-folders-subfolders-and-subfiles-using-pydrive-python

def parse_gdrive_path(gd_path):
    if ':' in gd_path:
        gd_path = gd_path.split(':')[1]
    gd_path = gd_path.replace('\\', '/').replace('//', '/')
    if gd_path.startswith('/'):
        gd_path = gd_path[1:]
    if gd_path.endswith('/'):
        gd_path = gd_path[:-1]
    return gd_path.split('/')

def resolve_path_to_id(folder_path,gdrive):
    _id = 'root'
    folder_path = parse_gdrive_path(folder_path)
    for idx, folder in enumerate(folder_path):
        folder_list = gdrive.ListFile({'q': f"'{_id}' in parents and title='{folder}' and trashed=false and mimeType='application/vnd.google-apps.folder'", 'fields': 'items(id, title, mimeType)'}).GetList()
        _id = folder_list[0]['id']
        title = folder_list[0]['title']
        if idx == (len(folder_path) - 1) and folder == title:
            return _id
    return _id



def get_folder_files(folder_ids, gdrive, batch_size=100):

    base_query = "'{target_id}' in parents"
    target_queries = []
    query = ''

    for idx, folder_id in enumerate(folder_ids):
        query += base_query.format(target_id=folder_id)
        if len(folder_ids) == 1 or idx > 0 and idx % batch_size == 0:
            target_queries.append(query)
            query = ''
        elif idx != len(folder_ids)-1:
            query += " or "
        else:
            target_queries.append(query)

    for query in target_queries:
        for f in gdrive.ListFile({'q': f"{query} and trashed=false", 'fields': 'items(id, title, mimeType, version)'}).GetList():
            yield f

def get_files(folder_path=None,gdrive = None, target_ids=None, files=[]):

    if target_ids is None:
        target_ids = [resolve_path_to_id(folder_path,gdrive)]

    file_list = get_folder_files(folder_ids=target_ids,gdrive=gdrive, batch_size=250)

    subfolder_ids = []

    for f in file_list:
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            subfolder_ids.append(f['id'])
        else:
            files.append(f['title'])

    if len(subfolder_ids) > 0:
        get_files(target_ids=subfolder_ids)

    return files

file_list = get_files('RegionalizationCollab/FSDS/temp_data/input/user_data_std/',drive)


for f in file_list:
    print(f)




# %% 
# https://stackoverflow.com/questions/34101427/accessing-folders-subfolders-and-subfiles-using-pydrive-python


def 


top_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
for file in top_list:
    if  file['title'] == 'RegionalizationCollab' :
        print('title: %s, id: %s' % (file['title'], file['id']))

        drive.ListFile({})
#Paginate file lists by specifying number of max results
for file_list in drive.ListFile({'q': 'trashed=true', 'maxResults': 100}):
    print('Received %s files from Files.list()' % len(file_list)) # <= 10
    for file1 in file_list:
        print('title: %s, id: %s' % (file1['title'], file1['id']))

#%%
# Step 2: Function to recursively list subfolders
def list_subfolders(parent_folder_id, indent=0):
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folder_list = drive.ListFile({'q': query}).GetList()

    for folder in folder_list:

        print(' ' * indent + f"Title: {folder['title']}, ID: {folder['id']}")
        # Recursively list subfolders of the current folder
        list_subfolders(folder['id'], indent + 4)

# Step 3: Specify the parent folder ID and list subfolders
parent_folder_id = 'RegionalizationCollab'#'root'  # Replace with your parent folder ID
ls_dir_struct = "RegionalizationCollab/FSDS/temp_data/input/user_data_std".split('/')
list_subfolders(parent_folder_id,ls_dir_struct)



ls_dir_struct = "RegionalizationCollab/FSDS/temp_data/input/user_data_std/test_folder_pydrive2".split('/')


print("Subfolders listed successfully.")

#%%

# List of titles to match
titles_to_match = ['Title1', 'Title2', 'Title3']  # Replace with your list of titles

# Step 2: Function to recursively list subfolders and check for matches
def list_and_match_subfolders(parent_folder_id, titles_to_match, indent=0):
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folder_list = drive.ListFile({'q': query}).GetList()

    for folder in folder_list:
        if folder['title'] in titles_to_match:
            print(' ' * indent + f"Matched Title: {folder['title']}, ID: {folder['id']}")
        else:
            print(' ' * indent + f"Title: {folder['title']}, ID: {folder['id']}")
        # Recursively list subfolders of the current folder
        list_and_match_subfolders(folder['id'], titles_to_match, indent + 4)

# Step 3: Specify the parent folder ID and list subfolders
parent_folder_id = 'your_parent_folder_id'  # Replace with your parent folder ID
list_and_match_subfolders(parent_folder_id, titles_to_match)

print("Subfolders listed successfully.")
Steps Explained: