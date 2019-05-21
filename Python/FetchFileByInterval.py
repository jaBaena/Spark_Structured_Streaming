import sys
import time
import urllib.request

if len(sys.argv) != 4:
    print("Usage: python FetchFileByInterval "
          "<URL to fetch> <streaming directory> <number of seconds>", file=sys.stderr)
    exit(-1)

urlToFetch = sys.argv[1]
streamingDirectory = sys.argv[2]
secondsToSleep = int(sys.argv[3])
while True:
    time_stamp = str(time.time())

    newFile = streamingDirectory + "/" + time_stamp + ".csv"
    urllib.request.urlretrieve(urlToFetch, newFile)

    time.sleep(secondsToSleep)
