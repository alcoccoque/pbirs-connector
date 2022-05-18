# library to manage all IO operations with the Power Bi Report Server
import os

import pandas as pd
import pyodbc
from repository import Repository


class PbiServer:
    def __init__(
        self,
        server=os.getenv("SERVER"),
        database=os.getenv("DATABASE"),
        username=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
    ):
        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._connection = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + self._server
            + ";DATABASE="
            + self._database
            + ";UID="
            + self._username
            + ";PWD="
            + self._password
            + ";"
        )

    def download_all_reports(self, repository: Repository):
        """
        Download all reports from pbi report server, by querying its BinaryContent from table catalogitemextendedcontent
        and saving its contents into a file.


        """
        all_reports_query = open("select_all_reports.sql", "r").read()
        report_content_query = open("select_report_content.sql", "r").read()
        reports = pd.read_sql_query(all_reports_query, self._connection)

        for report in reports.itertuples():
            print("Downloading {}".format(report.report_name))
            print(
                "{}% completed".format(
                    int(100 * float(report.Index + 1) / float(reports.shape[0]))
                )
            )
            os.sys.stdout.write("\r")
            # download report into reports Folder
            report_content = pd.read_sql_query(
                report_content_query, self._connection, params=[report.report_itemid]
            )
            input_filename = repository.reports + report.report_itemid + ".pbix"
            with open(input_filename, "wb") as pbix_file:
                pbix_file.write(report_content["BinaryContent"][0])
            pbix_file.close()
        del pbix_file
        del all_reports_query
        del report_content_query
        return None

    def get_report_list(self):
        all_reports_query = open("select_all_reports.sql", "r").read()
        return pd.read_sql_query(all_reports_query, self._connection)

    def get_folder_list(self):
        all_folders_query = open("select_all_folders.sql", "r").read()
        return pd.read_sql_query(all_folders_query, self._connection)


repository = Repository()
pbiserver = PbiServer(
    server="localhost\SQLEXPRESS",
    database="ReportServer",
    username="admin",
    password="123654",
)
pbiserver.download_all_reports(repository)
