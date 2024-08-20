#!/usr/bin/env python3

import os
import shutil
import hashlib
import time
import re
import itertools
import threading
import sys
import ssl
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from platform import python_version

################################################################
#
# Generated by: GDEX
# Created: 2024-08-02T14:18:50-06:00
#
# Your download selection includes data that might be secured using API Token based
# authentication. Therefore, this script can have your api-token. If you
# re-generate your API Token after you download this script, the download will
# fail. If that happens, you can either re-download the script or you can edit
# this script replacing the old API Token with the new one. View your API token
# by going to "Account Home":
#
# https://gdex.ucar.edu/account/user/account-home.html
#
# and clicking on the "API Token" link under "Personal Account". You will be asked
# to log into the application before you can view your API Token.
#
# Usage: python3 python-camels-20240802T1418.py
# Version: 1.0.1
#
# Dataset
# camels
# fbc54ccc-5184-4f54-b306-f58112a34700
# https://gdex.ucar.edu/dataset/camels.html
# https://gdex.ucar.edu/dataset/id/fbc54ccc-5184-4f54-b306-f58112a34700.html
#
# Dataset Version
# 1.2
# 0925542f-ede4-4f25-9424-3fce02d43240
# https://gdex.ucar.edu/dataset/camels/version/1.2.html
# https://gdex.ucar.edu/dataset/version/id/0925542f-ede4-4f25-9424-3fce02d43240.html
#
################################################################

print('Please email feedback to gdex@ucar.edu.\n')

data = [
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/basin_set_full_res.zip','filename':'basin_set_full_res.zip','bytes':'45179559','md5Checksum':'958fe520f6c4062dbddbbb67cfc28985'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_attributes_v2.0.pdf','filename':'camels_attributes_v2.0.pdf','bytes':'91532','md5Checksum':'77a6c084c798a31fbd05594ee58a90c7'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_attributes_v2.0.xlsx','filename':'camels_attributes_v2.0.xlsx','bytes':'16278','md5Checksum':'714c68bd5bb3314ca39b14f9467bd609'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_clim.txt','filename':'camels_clim.txt','bytes':'100673','md5Checksum':'67f22592f3fb72c57df81358ce68458b'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_geol.txt','filename':'camels_geol.txt','bytes':'71583','md5Checksum':'f5ce5de53eb1ea2532cda7e3b4813993'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_hydro.txt','filename':'camels_hydro.txt','bytes':'122799','md5Checksum':'55ebdeb36c42ee7acdb998229c3edb3a'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_name.txt','filename':'camels_name.txt','bytes':'30417','md5Checksum':'c96491b32c4df55a31bead7ceca7d64b'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_soil.txt','filename':'camels_soil.txt','bytes':'109125','md5Checksum':'8edb46a363a20b466a4b7105ba633767'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_topo.txt','filename':'camels_topo.txt','bytes':'38677','md5Checksum':'0f6267838c40b1507b64582433bc0b8e'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/camels_vege.txt','filename':'camels_vege.txt','bytes':'107970','md5Checksum':'f40e843defc1e654a800be9fe5fd5090'},
     {'url':'https://gdex.ucar.edu/api/v1/dataset/camels/file/readme.txt','filename':'readme.txt','bytes':'1704','md5Checksum':'b37d64950e9d4c5c10a8b4ef82bc6219'},]

def main(data):

    args = processArguments()

    for d in data:
        executeDownload(Download(args, d))

def processArguments():

    args = {}
    args.update({'apiToken': None})
    args.update({'userAgent': 'python/{}/gateway/{}'.format(python_version(), '4.4.10-20240516-151433')})
    args.update({'attemptMax': 10})
    args.update({'initialSleepSeconds': 10})
    args.update({'sleepMultiplier': 3})
    args.update({'sleepMaxSeconds': 900})
    args.update({'insecure': False})

    if '-k' in sys.argv or '--insecure' in sys.argv:
        args.update({'insecure': True})

    if '-h' in sys.argv or '--help' in sys.argv:
        print('Usage: {} [options...]'.format(sys.argv[0]))
        print(' -h, --help        Show usage')
        print(' -k, --insecure    Allow insecure server connections (no certificate check) when using SSL')
        exit(0)

    return args

def executeDownload(download):

    if not os.path.isfile(download.filename):
        attemptAndValidateDownload(download)
        moveDownload(download)
    else:
        download.success = True
        download.valid = True

    reportDownload(download)

def moveDownload(download):

    if download.success and (download.valid or download.vwarning):
        os.rename(download.filenamePart, download.filename)

def reportDownload(download):

    if download.success and download.valid:
        print('{} download successful'.format(download.filename))

    if download.success and not download.valid and download.vwarning:
        print('{} download validation warning: {}'.format(download.filename, download.vwarning))

    if download.success and not download.valid and download.verror:
        print('{} download validation error: {}'.format(download.filename, download.verror))

    if not download.success and download.error:
        print('{} download failed: {}'.format(download.filename, download.error))

def attemptAndValidateDownload(download):

    while download.attempt:
        downloadFile(download)

    if download.success:
        validateFile(download)

def downloadFile(download):

    try :
        startOrResumeDownload(download)
    except HTTPError as error:
        handleHTTPErrorAttempt(download, error)
    except URLError as error:
        handleRecoverableAttempt(download, error)
    except TimeoutError as error:
        handleRecoverableAttempt(download, error)
    except Exception as error:
        handleIrrecoverableAttempt(download, error)
    else:
        handleSuccessfulAttempt(download)

def startOrResumeDownload(download):

    startAnimateDownload('{} downloading:'.format(download.filename))

    if os.path.isfile(download.filenamePart):
        resumeDownloadFile(download)
    else:
        startDownloadFile(download)

def startAnimateDownload(message):
    global animateMessage
    global animateOn

    animateMessage = message
    animateOn = True

    # making the animation run as a daemon thread allows it to
    # exit when the parent (main) is terminated or killed
    t = threading.Thread(daemon=True, target=animateDownload)
    t.start()

def stopAnimateDownload(outcome):
    global animateOutcome
    global animateOn

    animateOutcome = outcome
    animateOn = False

    # wait for animation child process to stop before any parent print
    time.sleep(0.3)

def animateDownload():
    global animateMessage
    global animateOutcome
    global animateOn

    for d in itertools.cycle(['.  ', '.. ', '...', '   ']):

        if not animateOn:
            print('\r{} {}'.format(animateMessage, animateOutcome), flush=True)
            break

        print('\r{} {}'.format(animateMessage, d), end='', flush=True)
        time.sleep(0.2)

def resumeDownloadFile(download):

    request = createRequest(download, createResumeHeaders(download))
    readFile(download, request)

def startDownloadFile(download):

    request = createRequest(download, createStartHeaders(download))
    readFile(download, request)

def createResumeHeaders(download):

    headers = createStartHeaders(download)
    headers.update(createRangeHeader(download))

    return headers

def createRequest(download, headers):

    request = urllib.request.Request(download.url, headers=headers)

    return request

def createStartHeaders(download):

    headers = {}
    headers.update(createUserAgentHeader(download))

    if download.apiToken:
        headers.update(createAuthorizationHeader(download))

    return headers

def createUserAgentHeader(download):

    return {'User-agent': download.userAgent}

def createAuthorizationHeader(download):

    return {'Authorization': 'api-token {}'.format(download.apiToken)}

def createRangeHeader(download):

    start = os.path.getsize(download.filenamePart)
    header = {'Range': 'bytes={}-'.format(start)}

    return header

def readFile(download, request):

    context = createSSLContext(download)

    with urllib.request.urlopen(request, context=context) as response, open(download.filenamePart, 'ab') as fh:
        collectResponseHeaders(download, response)
        shutil.copyfileobj(response, fh)

def createSSLContext(download):

    # See:
    #      https://docs.python.org/3/library/urllib.request.html
    #      https://docs.python.org/3/library/http.client.html#http.client.HTTPSConnection
    #      https://docs.python.org/3/library/ssl.html#ssl.SSLContext
    #
    # Excerpts:
    #      If context is specified it must be a ssl.SSLContext instance...
    #      http.client.HTTPSConnection performs all the necessary certificate and hostname checks by default.

    if download.insecure:
        return ssl._create_unverified_context()

    return None

def collectResponseHeaders(download, response):

    download.responseHeaders = response.info()
    if download.responseHeaders.get('ETag'):
        download.etag = download.responseHeaders.get('ETag').strip('"')

def handleHTTPErrorAttempt(download, httpError):

    if httpError.code == 416: # 416 is Range Not Satisfiable
        # likely the file completely downloaded and validation was interrupted,
        # therefore calling it successfully downloaded and allowing validation
        # to say otherwise
        handleSuccessfulAttempt(download)
    else:
        handleRecoverableAttempt(download, httpError)

def handleRecoverableAttempt(download, error):

    stopAnimateDownload('error')

    print('failure on attempt {} downloading {}: {}'.format(download.attemptNumber, download.filename, error))

    if download.attemptNumber < download.attemptMax:
        sleepBeforeNextAttempt(download)
        download.attemptNumber += 1
    else:
        download.attempt = False
        download.error = error

def sleepBeforeNextAttempt(download):

    sleepSeconds = download.initialSleepSeconds * (download.sleepMultiplier ** (download.attemptNumber - 1))

    if sleepSeconds > download.sleepMaxSeconds:
        sleepSeconds = download.sleepMaxSeconds

    print('waiting {} seconds before next attempt to download {}'.format(sleepSeconds, download.filename))
    time.sleep(sleepSeconds)

def handleIrrecoverableAttempt(download, error):

    stopAnimateDownload('error')

    download.attempt = False
    download.error = error

def handleSuccessfulAttempt(download):

    stopAnimateDownload('done')

    download.attempt = False
    download.success = True

def validateFile(download):

    try:
        validateAllSteps(download)
    except InvalidDownload as error:
        download.valid = False
        download.vwarning = str(error)
    except Exception as error:
        download.valid = False
        download.verror = error
    else:
        download.valid = True

def validateAllSteps(download):

    verrorData = validatePerData(download)
    verrorEtag = validatePerEtag(download)
    verrorStale = validateStaleness(download)

    if verrorData and verrorEtag:
        raise verrorData

    if verrorStale:
        raise verrorStale

def validatePerData(download):

    try:
        validateBytes(download)
        validateChecksum(download)
    except InvalidDownload as error:
        return error
    else:
        return None

def validateBytes(download):

    size = os.path.getsize(download.filenamePart)
    if not download.bytes == size:
        raise InvalidSizeValue(download, size)

def validateChecksum(download):

    if download.md5Checksum:
        md5Checksum = readMd5Checksum(download)
        if not download.md5Checksum == md5Checksum:
            raise InvalidChecksumValue(download, md5Checksum)
    else:
        raise UnableToPerformChecksum(download)

def readMd5Checksum(download):

    hash_md5 = hashlib.md5()

    with open(download.filenamePart, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

def validatePerEtag(download):

    try:
        validateChecksumEtag(download)
    except InvalidDownload as error:
        return error
    else:
        return None

def validateChecksumEtag(download):

    if isEtagChecksum(download):
        md5Checksum = readMd5Checksum(download)
        if not download.etag == md5Checksum:
            raise InvalidChecksumValuePerEtag(download, md5Checksum)
    else:
        raise UnableToPerformChecksum(download)

def isEtagChecksum(download):

    return download.etag and re.fullmatch(r'[a-z0-9]+', download.etag)

def validateStaleness(download):

    try:
        validateStaleChecksum(download)
    except InvalidDownload as error:
        return error
    else:
        return None

def validateStaleChecksum(download):

    if isEtagChecksum(download):
        if not download.md5Checksum or download.md5Checksum != download.etag:
            raise StaleChecksumValue(download)

class InvalidDownload(Exception):

    pass

class InvalidSizeValue(InvalidDownload):

    def __init__(self, download, actual):
        super().__init__('invalid byte size: downloaded file is {} bytes but should be {}'.format(actual, download.bytes))

class InvalidChecksumValue(InvalidDownload):

    def __init__(self, download, actual):
        super().__init__('invalid checksum: downloaded file is {} but should be {}'.format(actual, download.md5Checksum))

class InvalidChecksumValuePerEtag(InvalidDownload):

    def __init__(self, download, actual):
        super().__init__('invalid checksum: downloaded file is {} but should be {} according to server'.format(actual, download.etag))

class UnableToPerformChecksum(InvalidDownload):

    def __init__(self, download):
        super().__init__('cannot verify checksum')

class StaleChecksumValue(InvalidDownload):

    def __init__(self, download):
        super().__init__('checksum value has changed')

class Download():

    def __init__(self, args, datum):

        self.apiToken = args.get('apiToken')
        self.userAgent = args.get('userAgent')
        self.attemptMax = args.get('attemptMax')
        self.initialSleepSeconds = args.get('initialSleepSeconds')
        self.sleepMultiplier = args.get('sleepMultiplier')
        self.sleepMaxSeconds = args.get('sleepMaxSeconds')
        self.insecure = args.get('insecure')

        self.url = datum.get('url')
        self.filename = datum.get('filename')
        self.bytes = int(datum.get('bytes'))
        self.md5Checksum = datum.get('md5Checksum')

        self.filenamePart = self.filename + '.part'
        self.success = False
        self.attempt = True
        self.attemptNumber = 1
        self.responseHeaders = {}
        self.etag = None
        self.error = None
        self.valid = False
        self.vwarning = None
        self.verror = None

    def __str__(self):
        return f'url: {self.url}, filename: {self.filename}, bytes: {self.bytes}, md5Checksum: {self.md5Checksum}'

if __name__ == '__main__':
    main(data)
