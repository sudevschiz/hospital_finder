# https://gist.github.com/nickjevershed/332d1fa264d1d7d93e95

import json
import requests


def fetch():
    # Sheet key
    key = "1IWjEQGUAQpQfT_wWVDiQqUoK457bE_MnTbpgnBPzTiE"
    # sheet_id = "od6"
    sheet_id = "ov0t4ow"
    # Google api request urls
    url = (
        "https://spreadsheets.google.com/feeds/list/"
        + key
        + f"/{sheet_id}/public/values?alt=json"
    )

    # Lists to store new keys and data
    newKeys = []
    newData = []

    # Get json in list format

    ssContent = requests.get(url).json()

    # Remap entries from having gsx$-prefixed keys to having no prefix, ie our first row as keys
    firstrow = ssContent["feed"]["entry"][0]
    newKeys = [key.replace("gsx$", "") for key in firstrow.keys() if "gsx$" in key]

    # Read each row and populate the dictionary
    for entry in ssContent["feed"]["entry"]:
        rowData = []
        for key in newKeys:
            rowData.append(entry["gsx$" + key]["$t"])
        newData.append(dict(zip(newKeys, rowData)))

    # Saves the json file locally as output.json.
    with open("output.json", "w") as fileOut:
        json.dump(newData, fileOut, indent=4)

    return newData


if __name__ == "__main__":
    fetch()
