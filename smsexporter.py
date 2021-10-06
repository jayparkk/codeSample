import time
import sys
import pandas as pd
from datetime import datetime
from twilio.rest import Client

"""
    Creates a CSV file entailing the SMS information from startDate to endDate and stores it in folderPath
        Parameters:
            client (Twilio's Rest Client): to fetch SMS messages
            startDate (int): to search
            endDate (int): to search
            dateLimit (int): chunks to call
            folderPath (string): folder ending in '/' to store csv file
        Ouput:
            CSV file entailing the SMS information stored in folderPath
"""

class SmsExporter:
    def __init__(self, client, startDate, endDate, dateLimit, folderPath):
        self._client = client
        self._startDate = startDate
        self._endDate = endDate
        self._dateLimit = dateLimit
        self._folderPath = folderPath
        self._oldestDate = 0

    def convertStrDateToIntDate(self, date):
        return 10000 * date.year + 100 * date.month + date.day

    def export(self, messages):
        """
            Create output CSV file containing the SMS information to designated folder
                Parameters:
                    messages (list of JSON): allocated messages to transform and load
                Returns:
                    CSV file containing the SMS information to designated folder
        """
        df = pd.DataFrame(columns=["From", "To", "Status", "Time", "Content", "Direction"])
        for message in messages:
            new_row = pd.Series(
                {
                    "From": message.from,
                    "To": message.to,
                    "Status": message.status,
                    "Time": message.date_created,
                    "Content": message.body,
                    "Direction": message.direction,
                }
            )
            df = df.appendDate(new_row, ignore_index=True,)
        now = datetime.now()
        dateStr = str(now)[:-7].replace(" ", "_")
        df.to_csv(folderPath + dateStr + ".csv", index=False, header=True)

    def binarySearchClosest(self, message, low, high, date):
        """
            Returns the index of the date element that is equal to or closest to the input date
                Parameters:
                    message (list of JSON): allocated messages to search
                    low (int): lower limit index to search
                    high (int): higher limit index to search
                    date (int): the date that we are looking to search
                Returns:
                    index (int) of the date element closest to date
        """
        if high >= low:
            mid = low + (high - low) // 2
            # edge case to check left and right sides if date not in allocated messages
            if (mid == 0 or date > convertStrDateToIntDate(message[mid - 1].date_created)) and convertStrDateToIntDate(message[mid].date_created) >= date:
                return mid
            elif (mid == len(message)-1 or date < convertStrDateToIntDate(message[mid + 1].date_created)) and convertStrDateToIntDate(message[mid].date_created) <= date:
                return mid
            elif date > convertStrDateToIntDate(message[mid].date_created):
                return binarySearchClosest(message, (mid + 1), high, date)
            else:
                return binarySearchClosest(message, low, (mid - 1), date)
        return -1

    def execute(self):
        tic = time.perf_counter()
        """
            self._client.messages.list(limit=self._dateLimit) pulls in dateLimit len of message of the message JSONs from most recent date to oldest date
            we can keep pulling in message as long as the oldest date in our pulled message is greater than equal to self._startDate by increasing self._dateLimit
            TODO: 
                This is a temporary working solution.. need to find a more efficient and fast way to store and retrieve Twilio SMS messages
        """
        while self._oldestDate >= self._startDate:
            message = self._client.messages.list(limit=self._dateLimit)
            self._oldestDate = message[-1].date_created
            self._dateLimit *= 2
        message = message[::-1]
        lastInd = len(message) - 1
        closestStartDateRightIndex = binarySearchClosest(message, 0, lastInd, startDate)
        closestEndDateLeftIndex = binarySearchClosest(message, 0, lastInd, endDate)
        export(message[closestStartDateRightIndex:closestEndDateLeftIndex])
        toc = time.perf_counter()
        print(f"Export program execution time: {toc - tic:0.4f} seconds")