#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################

import logging
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, List, Optional

import datahub.emitter.mce_builder as builder
import requests
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (SourceCapability, SupportStatus,
                                              capability, config_class,
                                              platform_name, support_status)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass, CorpUserInfoClass, CorpUserKeyClass, StatusClass)
from pydantic.fields import Field
# Logger instance
from requests_ntlm import HttpNtlmAuth

from powerbireportserver.src.api_domain import (DataSet, DataSource, LinkedReport,
                                                MetaData, MobileReport,
                                                PowerBiReport, Report,
                                                SystemPolicies)

LOGGER = logging.getLogger(__name__)


class Constant:
    """
    keys used in powerbi plugin
    """
    DATASET = "DATASET"
    REPORTS = "REPORTS"
    REPORT = "REPORT"
    DATASOURCE = "DATASOURCE"
    DATASET_DATASOURCES = "DATASET_DATASOURCES"
    DatasetId = "DatasetId"
    ReportId = "ReportId"
    PowerBiReportId = "ReportId"
    Dataset_URN = "DatasetURN"
    DASHBOARD_ID = "powerbi.linkedin.com/dashboards/{}"
    DASHBOARD = "dashboard"
    DATASETS = "DATASETS"
    DATASET_ID = "powerbi.linkedin.com/datasets/{}"
    DATASET_PROPERTIES = "datasetProperties"
    SUBSCRIPTION = "SUBSCRIPTION"
    SYSTEM = "SYSTEM"
    CATALOG_ITEM = "CATALOG_ITEM"
    EXCEL_WORKBOOK = "EXCEL_WORKBOOK"
    EXTENSIONS = "EXTENSIONS"
    FAVORITE_ITEM = "FAVORITE_ITEM"
    FOLDERS = "FOLDERS"
    KPIS = "KPIS"
    LINKED_REPORTS = "LINKED_REPORTS"
    LINKED_REPORT = "LINKED_REPORT"
    ME = "ME"
    MOBILE_REPORTS = "MOBILE_REPORTS"
    MOBILE_REPORT = "MOBILE_REPORT"
    POWERBI_REPORTS = "POWERBI_REPORTS"
    POWERBI_REPORT = "POWERBI_REPORT"
    RESOURCE = "RESOURCE"
    SESSION = "SESSION"
    SYSTEM_POLICIES = "SYSTEM_POLICIES"

    DATASET_KEY = "datasetKey"
    BROWSERPATH = "browsePaths"
    DATAPLATFORM_INSTANCE = "dataPlatformInstance"
    STATUS = "status"
    VALUE = "value"
    ID = "ID"
    # OWNERSHIP = "ownership"
    # DASHBOARD_KEY = "dashboardKey"
    # CHART_ID = "powerbi.linkedin.com/charts/{}"
    # CHART_KEY = "chartKey"
    # PBIAccessToken = "PBIAccessToken"
    # DASHBOARD_LIST = "DASHBOARD_LIST"
    # TILE_LIST = "TILE_LIST"
    # CHART = "chart"
    # CHART_URN = "ChartURN"
    # CHART_INFO = "chartInfo"
    # CORP_USER = "corpuser"
    # CORP_USER_INFO = "corpUserInfo"
    # CORP_USER_KEY = "corpUserKey"
    # TILE_GET = "TILE_GET"
    # ENTITY_USER_LIST = "ENTITY_USER_LIST"
    # SCAN_CREATE = "SCAN_CREATE"
    # SCAN_GET = "SCAN_GET"
    # SCAN_RESULT_GET = "SCAN_RESULT_GET"
    # Authorization = "Authorization"
    # WorkspaceId = "WorkspaceId"
    # DashboardId = "DashboardId"
    # ENTITY = "ENTITY"
    # DASHBOARD_INFO = "dashboardInfo"
    # SCAN_ID = "ScanId"


class PowerBiReportServerAPIConfig(EnvBasedSourceConfigBase):
    # # Organsation Identifier
    # tenant_id: str = Field(description="Power BI tenant identifier.")
    # # PowerBi workspace identifier
    # workspace_id: str = Field(description="Power BI workspace identifier.")
    # # Dataset type mapping
    # dataset_type_mapping: Dict[str, str] = Field(
    #     description="Mapping of Power BI datasource type to Datahub dataset."
    # )
    # # Azure app client identifier
    # client_id: str = Field(description="Azure AD App client identifier.")
    # # Azure app client secret
    # client_secret: str = Field(description="Azure AD App client secret.")
    # timeout for meta-data scanning
    username: str = Field(description="Windows account username")
    password: str = Field(description="Windows account password")
    workstation_name: str = Field(default="localhost", description="Workstation name")
    report_virtual_directory_name: str = Field(
        description="Report Virtual Directory URL name"
    )
    report_server_virtual_directory_name: str = Field(
        description="Report Server Virtual Directory URL name"
    )
    dataset_type_mapping: Dict[str, str] = Field(
        description="Mapping of Power BI datasource type to Datahub dataset."
    )
    scan_timeout: int = Field(
        default=60,
        description="time in seconds to wait for Power BI metadata scan result.",
    )

    # scope: str = "https://analysis.windows.net/powerbi/api/.default"
    # base_url: str = "http://{}/{}/api/v2.0/"
    # admin_base_url: str = "https://api.powerbi.com/v1.0/myorg/admin"
    # authority: str = "https://login.microsoftonline.com/"

    @property
    def get_base_api_url(self):
        return "http://{}/{}/api/v2.0/".format(
            self.workstation_name, self.report_virtual_directory_name
        )


class PowerBiDashboardSourceConfig(PowerBiReportServerAPIConfig):
    platform_name: str = "powerbireportserver"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
    report_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


class PowerBiReportServerAPI:
    # API endpoints of PowerBi Report Server to fetch reports, datasets
    API_ENDPOINTS = {
        Constant.CATALOG_ITEM: "{PBIRS_BASE_URL}/CatalogItems({CATALOG_ID})",
        Constant.DATASETS: "{PBIRS_BASE_URL}/Datasets",
        Constant.DATASET: "{PBIRS_BASE_URL}/Datasets({DATASET_ID})",
        Constant.DATASET_DATASOURCES: "{PBIRS_BASE_URL}/Datasets({DATASET_ID})/DataSources",
        Constant.DATASOURCE: "{PBIRS_BASE_URL}/DataSources({DATASOURCE_ID})",
        Constant.EXCEL_WORKBOOK: "{PBIRS_BASE_URL}/ExcelWorkbooks({EXCEL_WORKBOOK_ID})",
        Constant.EXTENSIONS: "{PBIRS_BASE_URL}/Extensions",
        Constant.FAVORITE_ITEM: "{PBIRS_BASE_URL}/FavoriteItems({FAVORITE_ITEM_ID})",
        Constant.FOLDERS: "{PBIRS_BASE_URL}/Folders({FOLDER_ID})",
        Constant.KPIS: "{PBIRS_BASE_URL}/Kpis({KPI_ID})",
        Constant.LINKED_REPORTS: "{PBIRS_BASE_URL}/LinkedReports",
        Constant.LINKED_REPORT: "{PBIRS_BASE_URL}/LinkedReports({LINKED_REPORT_ID})",
        Constant.ME: "{PBIRS_BASE_URLL}/Me",
        Constant.MOBILE_REPORTS: "{PBIRS_BASE_URL}/MobileReports",
        Constant.MOBILE_REPORT: "{PBIRS_BASE_URL}/MobileReports({MOBILE_REPORT_ID})",
        Constant.POWERBI_REPORTS: "{PBIRS_BASE_URL}/PowerBiReports",
        Constant.POWERBI_REPORT: "{PBIRS_BASE_URL}/PowerBiReports({POWERBI_REPORT_ID})",
        Constant.REPORTS: "{PBIRS_BASE_URL}/Reports",
        Constant.REPORT: "{PBIRS_BASE_URL}/Reports({REPORT_ID})",
        Constant.RESOURCE: "{PBIRS_BASE_URL}/Resources({RESOURCE_GET})",
        Constant.SESSION: "{PBIRS_BASE_URL}/Session",
        Constant.SUBSCRIPTION: "{PBIRS_BASE_URL}/Subscriptions({SUBSCRIPTION_ID})",
        Constant.SYSTEM: "{PBIRS_BASE_URL}/System",
        Constant.SYSTEM_POLICIES: "{PBIRS_BASE_URL}/System/Policies",
    }

    def __init__(self, config: PowerBiReportServerAPIConfig) -> None:
        self.__config: PowerBiReportServerAPIConfig = config
        self.__auth: HttpNtlmAuth = HttpNtlmAuth(
            "{}\\{}".format(self.__config.workstation_name, self.__config.username),
            self.__config.password,
        )

    def get_auth_credentials(self):
        return self.__auth

    def get_users_policies(self) -> List[SystemPolicies]:
        """
        Get user policy by Power Bi Report Server System
        """
        user_list_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.SYSTEM_POLICIES
        ]
        # Replace place holders
        user_list_endpoint = user_list_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url)
        # Hit PowerBi
        LOGGER.info("Request to URL={}".format(user_list_endpoint))
        response = requests.get(
            url=user_list_endpoint,
            auth=self.get_auth_credentials())

        # Check if we got response from PowerBi
        if response.status_code != 200:
            LOGGER.warning(
                "Failed to fetch user list from power-bi for, http_status={}, message={}".format(
                    response.status_code, response.text
                )
            )
            raise ConnectionError("Failed to fetch the user list from the power-bi")

        users_dict: List[Any] = response.json()[Constant.VALUE]
        # Iterate through response and create a list of PowerBiAPI.Dashboard
        users: List[SystemPolicies] = [SystemPolicies.parse_obj(instance) for instance in users_dict]
        return users

    def __get_report(self, report_id: str) -> Optional[Report]:
        """
        Fetch the .rdl report from PowerBiReportServer for the given report id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        report_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.REPORT
        ]
        # Replace place holders
        report_get_endpoint = report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to report URL={}".format(report_get_endpoint))
        response = requests.get(
            url=report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return Report.parse_obj(response_dict)

    def get_powerbi_report(self, report_id: str) -> Optional[PowerBiReport]:
        """
        Fetch the .pbix report from PowerBiReportServer for the given report id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        powerbi_report_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.POWERBI_REPORT
        ]
        # Replace place holders
        powerbi_report_get_endpoint = powerbi_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            POWERBI_REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to report URL={}".format(powerbi_report_get_endpoint))
        response = requests.get(
            url=powerbi_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()
        return PowerBiReport.parse_obj(response_dict)


    def get_linked_report(self, report_id: str) -> Optional[LinkedReport]:
        """
        Fetch the mobile report from PowerBiReportServer for the given report id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        linked_report_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.LINKED_REPORT
        ]
        # Replace place holders
        linked_report_get_endpoint = linked_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            LINKED_REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to report URL={}".format(linked_report_get_endpoint))
        response = requests.get(
            url=linked_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return LinkedReport.parse_obj(response_dict)

    def get_mobile_report(self, report_id: str) -> Optional[MobileReport]:
        """
        Fetch the mobile report from PowerBiReportServer for the given report id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        mobile_report_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.MOBILE_REPORT
        ]
        # Replace place holders
        mobile_report_get_endpoint = mobile_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            MOBILE_REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to report URL={}".format(mobile_report_get_endpoint))
        response = requests.get(
            url=mobile_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return MobileReport.parse_obj(response_dict)

    def get_all_reports(self) -> List[Any]:
        """
        Fetch all reports from PowerBiReportServer for the given report id
        """
        report_types_mapping = {
            Constant.REPORTS: Report,
            Constant.MOBILE_REPORTS: MobileReport,
            Constant.LINKED_REPORTS: LinkedReport,
            Constant.POWERBI_REPORTS: PowerBiReport,
        }

        reports = []
        for report_type in report_types_mapping.keys():

            report_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[report_type]
            # Replace place holders
            report_get_endpoint = report_get_endpoint.format(
                PBIRS_BASE_URL=self.__config.get_base_api_url,
            )
            # Hit PowerBiReportServer
            LOGGER.info("Request to report URL={}".format(report_get_endpoint))
            response = requests.get(
                url=report_get_endpoint,
                auth=self.get_auth_credentials(),
            )

            # Check if we got response from PowerBi
            if response.status_code != 200:
                message: str = "Failed to fetch report from power-bi-report-server for"
                LOGGER.warning(message)
                LOGGER.warning("{}={}".format(Constant.ReportId, report_type))

            response_dict = response.json()["value"]
            if response_dict:
                reports.extend(report_types_mapping[report_type].parse_obj(report) for report in response_dict)

        return reports


    # def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
    #     """
    #     Return list of dashboard users
    #     """
    #     return self.__get_users(
    #         workspace_id=dashboard.workspace_id, entity="dashboards", id=dashboard.id
    #     )

    # def get_dashboards(self, workspace: Workspace) -> List[Dashboard]:
    #     """
    #     Get the list of dashboard from PowerBi for the given workspace identifier
    #
    #     TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get-dashboards), there is no information available on pagination
    #     """
    #     dashboard_list_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
    #         Constant.DASHBOARD_LIST
    #     ]
    #     # Replace place holders
    #     dashboard_list_endpoint = dashboard_list_endpoint.format(
    #         POWERBI_BASE_URL=self.__config.get_base_url, WORKSPACE_ID=workspace.id
    #     )
    #     # Hit PowerBi
    #     LOGGER.info("Request to URL={}".format(dashboard_list_endpoint))
    #     response = requests.get(
    #         url=dashboard_list_endpoint,
    #         headers={Constant.Authorization: self.get_access_token()},
    #     )
    #
    #     # Check if we got response from PowerBi
    #     if response.status_code != 200:
    #         LOGGER.warning("Failed to fetch dashboard list from power-bi for")
    #         LOGGER.warning("{}={}".format(Constant.WorkspaceId, workspace.id))
    #         raise ConnectionError(
    #             "Failed to fetch the dashboard list from the power-bi"
    #         )
    #
    #     dashboards_dict: List[Any] = response.json()[Constant.VALUE]
    #
    #     # Iterate through response and create a list of PowerBiAPI.Dashboard
    #     dashboards: List[PowerBiReportServerAPI.Dashboard] = [
    #         PowerBiReportServerAPI.Dashboard(
    #             id=instance.get("id"),
    #             isReadOnly=instance.get("isReadOnly"),
    #             displayName=instance.get("displayName"),
    #             embedUrl=instance.get("embedUrl"),
    #             webUrl=instance.get("webUrl"),
    #             workspace_id=workspace.id,
    #             workspace_name=workspace.name,
    #             tiles=[],
    #             users=[],
    #         )
    #         for instance in dashboards_dict
    #         if instance is not None
    #     ]
    #
    #     return dashboards

    def get_dataset(self, dataset_id: str) -> Any:
        """
        Fetch the dataset from PowerBi for the given dataset identifier
        """
        if dataset_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.DatasetId, dataset_id))
            return None

        dataset_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.DATASET
        ]
        # Replace place holders
        dataset_get_endpoint = dataset_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            DATASET_ID=dataset_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to dataset URL={}".format(dataset_get_endpoint))
        response = requests.get(url=dataset_get_endpoint, auth=self.get_auth_credentials())
        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch dataset from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.DatasetId, dataset_id))
            raise ConnectionError(message)

        response_dict = response.json()

        # PowerBi Always return the webURL, in-case if it is None then setting complete webURL to None instead of None/details
        return DataSet.parse_obj(response_dict)

    def get_data_source(self, dataset: DataSet) -> Any:
        """
        Fetch the data source from PowerBi for the given dataset
        """

        datasource_get_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.DATASET_DATASOURCES
        ]
        # Replace place holders
        datasource_get_endpoint = datasource_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            DATASET_ID=dataset.Id,
        )
        # Hit PowerBi
        LOGGER.info("Request to datasource URL={}".format(datasource_get_endpoint))
        response = requests.get(url=datasource_get_endpoint, auth=self.get_auth_credentials())
        # Check if we got response from PowerBi
        if response.status_code != 200:
            message: str = "Failed to fetch datasource from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.DatasetId, dataset.Id))
            raise ConnectionError(message)

        res = response.json()
        value = res["value"]
        if len(value) == 0:
            LOGGER.info(
                "datasource is not found for dataset {}({})".format(
                    dataset.Name, dataset.Id
                )
            )
            return None
        # Consider only zero index datasource
        datasource_dict = value[0]
        # Create datasource instance with basic detail available
        datasource = DataSource.parse_obj(datasource_dict)

        # Check if datasource is relational as per our relation mapping
        if self.__config.dataset_type_mapping.get(datasource.Type) is not None:
            # Now set the database detail as it is relational data source
            datasource.MetaData = MetaData(is_relational=True)
            datasource.database = datasource_dict["connectionDetails"]["database"]
            datasource.server = datasource_dict["connectionDetails"]["server"]
        else:
            datasource.MetaData = MetaData(is_relational=False)

        return datasource

    # def get_tiles(self, workspace: Workspace, dashboard: Dashboard) -> List[Tile]:
    #
    #     """
    #     Get the list of tiles from PowerBi for the given workspace identifier
    #
    #     TODO: Pagination. As per REST API doc (https://docs.microsoft.com/en-us/rest/api/power-bi/dashboards/get-tiles), there is no information available on pagination
    #     """
    #
    #     def new_dataset_or_report(tile_instance: Any) -> dict:
    #         """
    #         Find out which is the data source for tile. It is either REPORT or DATASET
    #         """
    #         report_fields = {
    #             "dataset": None,
    #             "report": None,
    #             "createdFrom": PowerBiReportServerAPI.Tile.CreatedFrom.UNKNOWN,
    #         }
    #
    #         report_fields["dataset"] = (
    #             workspace.datasets[tile_instance.get("datasetId")]
    #             if tile_instance.get("datasetId") is not None
    #             else None
    #         )
    #         report_fields["report"] = (
    #             self.__get_report(
    #                 workspace_id=workspace.id,
    #                 report_id=tile_instance.get("reportId"),
    #             )
    #             if tile_instance.get("reportId") is not None
    #             else None
    #         )
    #
    #         # Tile is either created from report or dataset or from custom visualization
    #         report_fields[
    #             "createdFrom"
    #         ] = PowerBiReportServerAPI.Tile.CreatedFrom.UNKNOWN
    #         if report_fields["report"] is not None:
    #             report_fields[
    #                 "createdFrom"
    #             ] = PowerBiReportServerAPI.Tile.CreatedFrom.REPORT
    #         elif report_fields["dataset"] is not None:
    #             report_fields[
    #                 "createdFrom"
    #             ] = PowerBiReportServerAPI.Tile.CreatedFrom.DATASET
    #         else:
    #             report_fields[
    #                 "createdFrom"
    #             ] = PowerBiReportServerAPI.Tile.CreatedFrom.VISUALIZATION
    #
    #         LOGGER.info(
    #             "Tile {}({}) is created from {}".format(
    #                 tile_instance.get("title"),
    #                 tile_instance.get("id"),
    #                 report_fields["createdFrom"],
    #             )
    #         )
    #
    #         return report_fields
    #
    #     tile_list_endpoint: str = PowerBiReportServerAPI.API_ENDPOINTS[
    #         Constant.TILE_LIST
    #     ]
    #     # Replace place holders
    #     tile_list_endpoint = tile_list_endpoint.format(
    #         POWERBI_BASE_URL=self.__config.get_base_url,
    #         WORKSPACE_ID=dashboard.workspace_id,
    #         DASHBOARD_ID=dashboard.id,
    #     )
    #     # Hit PowerBi
    #     LOGGER.info("Request to URL={}".format(tile_list_endpoint))
    #     response = requests.get(
    #         url=tile_list_endpoint,
    #         headers={Constant.Authorization: self.get_access_token()},
    #     )
    #
    #     # Check if we got response from PowerBi
    #     if response.status_code != 200:
    #         LOGGER.warning("Failed to fetch tiles list from power-bi for")
    #         LOGGER.warning("{}={}".format(Constant.WorkspaceId, workspace.id))
    #         LOGGER.warning("{}={}".format(Constant.DashboardId, dashboard.id))
    #         raise ConnectionError("Failed to fetch the tile list from the power-bi")
    #
    #     # Iterate through response and create a list of PowerBiAPI.Dashboard
    #     tile_dict: List[Any] = response.json()[Constant.VALUE]
    #     tiles: List[PowerBiReportServerAPI.Tile] = [
    #         PowerBiReportServerAPI.Tile(
    #             id=instance.get("id"),
    #             title=instance.get("title"),
    #             embedUrl=instance.get("embedUrl"),
    #             **new_dataset_or_report(instance),
    #         )
    #         for instance in tile_dict
    #         if instance is not None
    #     ]
    #
    #     return tiles

    # flake8: noqa: C901
    # def get_workspace(self, workspace_id: str) -> Workspace:
    #     """
    #     Return Workspace for the given workspace identifier i.e workspace_id
    #     """
    #     scan_create_endpoint = PowerBiReportServerAPI.API_ENDPOINTS[
    #         Constant.SCAN_CREATE
    #     ]
    #     scan_create_endpoint = scan_create_endpoint.format(
    #         POWERBI_ADMIN_BASE_URL=self.__config.admin_base_url
    #     )
    #
    #     def create_scan_job():
    #         """
    #         Create scan job on PowerBi for the workspace
    #         """
    #         request_body = {"workspaces": [workspace_id]}
    #
    #         res = requests.post(
    #             scan_create_endpoint,
    #             data=request_body,
    #             params={
    #                 "datasetExpressions": True,
    #                 "datasetSchema": True,
    #                 "datasourceDetails": True,
    #                 "getArtifactUsers": True,
    #                 "lineage": True,
    #             },
    #             headers={Constant.Authorization: self.get_access_token()},
    #         )
    #
    #         if res.status_code not in (200, 202):
    #             message = "API({}) return error code {} for workpace id({})".format(
    #                 scan_create_endpoint, res.status_code, workspace_id
    #             )
    #
    #             LOGGER.warning(message)
    #
    #             raise ConnectionError(message)
    #         # Return Id of Scan created for the given workspace
    #         id = res.json()["id"]
    #         LOGGER.info("Scan id({})".format(id))
    #         return id

    # def wait_for_scan_to_complete(self, scan_id: str, timeout: int) -> Boolean:
    #     """
    #     Poll the PowerBi service for workspace scan to complete
    #     """
    #     minimum_sleep = 3
    #     if timeout < minimum_sleep:
    #         LOGGER.info(
    #             "Setting timeout to minimum_sleep time {} seconds".format(minimum_sleep)
    #         )
    #         timeout = minimum_sleep
    #
    #     max_trial = int(timeout / minimum_sleep)
    #     LOGGER.info("Max trial {}".format(max_trial))
    #     scan_get_endpoint = PowerBiReportServerAPI.API_ENDPOINTS[Constant.SCAN_GET]
    #     scan_get_endpoint = scan_get_endpoint.format(
    #         POWERBI_ADMIN_BASE_URL=self.__config.admin_base_url, SCAN_ID=scan_id
    #     )
    #
    #     LOGGER.info("Hitting URL={}".format(scan_get_endpoint))
    #
    #     trail = 1
    #     while True:
    #         LOGGER.info("Trial = {}".format(trail))
    #         res = requests.get(
    #             scan_get_endpoint,
    #             headers={Constant.Authorization: self.get_access_token()},
    #         )
    #         if res.status_code != 200:
    #             message = "API({}) retu rn error code {} for scan id({})".format(
    #                 scan_get_endpoint, res.status_code, scan_id
    #             )
    #
    #             LOGGER.warning(message)
    #
    #             raise ConnectionError(message)
    #
    #         if res.json()["status"].upper() == "Succeeded".upper():
    #             LOGGER.info("Scan result is available for scan id({})".format(scan_id))
    #             return True
    #
    #         if trail == max_trial:
    #             break
    #         LOGGER.info("Sleeping for {} seconds".format(minimum_sleep))
    #         sleep(minimum_sleep)
    #         trail += 1
    #
    #     # Result is not available
    #     return False

    def get_scan_result(self, scan_id: str) -> dict:
        LOGGER.info("Fetching scan  result")
        LOGGER.info("{}={}".format(Constant.SCAN_ID, scan_id))
        scan_result_get_endpoint = PowerBiReportServerAPI.API_ENDPOINTS[
            Constant.SCAN_RESULT_GET
        ]
        scan_result_get_endpoint = scan_result_get_endpoint.format(
            POWERBI_ADMIN_BASE_URL=self.__config.admin_base_url, SCAN_ID=scan_id
        )

        LOGGER.info("Hittin URL={}".format(scan_result_get_endpoint))
        res = requests.get(
            scan_result_get_endpoint,
            headers={Constant.Authorization: self.get_access_token()},
        )
        if res.status_code != 200:
            message = "API({}) return error code {} for scan id({})".format(
                scan_result_get_endpoint, res.status_code, scan_id
            )

            LOGGER.warning(message)

            raise ConnectionError(message)

        return res.json()["workspaces"][0]

    # def json_to_dataset_map(self, scan_result: dict) -> dict:
    #     """
    #     Filter out "dataset" from scan_result and return PowerBiAPI.Dataset instance set
    #     """
    #     datasets: Optional[Any] = scan_result.get("datasets")
    #     dataset_map: dict = {}
    #
    #     if datasets is None or len(datasets) == 0:
    #         LOGGER.warning(
    #             "Workspace {}({}) does not have datasets".format(
    #                 scan_result["name"], scan_result["id"]
    #             )
    #         )
    #         LOGGER.info("Returning empty datasets")
    #         return dataset_map
    #
    #     for dataset_dict in datasets:
    #         dataset_instance: Dataset = self.get_dataset(
    #             dataset_id=dataset_dict["id"],
    #         )
    #
    #         dataset_map[dataset_instance.id] = dataset_instance
    #         # set dataset's DataSource
    #         dataset_instance.datasource = self.get_data_source(dataset_instance)
    #         # Set table only if the datasource is relational and dataset is not created from custom SQL i.e Value.NativeQuery(
    #         # There are dataset which doesn't have DataSource
    #         if (
    #             dataset_instance.datasource
    #             and dataset_instance.datasource.metadata.is_relational is True
    #         ):
    #             LOGGER.info(
    #                 "Processing tables attribute for dataset {}({})".format(
    #                     dataset_instance.name, dataset_instance.id
    #                 )
    #             )
    #
    #             for table in dataset_dict["tables"]:
    #                 if "Value.NativeQuery(" in table["source"][0]["expression"]:
    #                     LOGGER.warning(
    #                         "Table {} is created from Custom SQL. Ignoring in processing".format(
    #                             table["name"]
    #                         )
    #                     )
    #                     continue
    #
    #                 # PowerBi table name contains schema name and table name. Format is <SchemaName> <TableName>
    #                 schema_and_name = table["name"].split(" ")
    #                 dataset_instance.tables.append(
    #                     Dataset.Table(
    #                         schema_name=schema_and_name[0],
    #                         name=schema_and_name[1],
    #                     )
    #                 )
    #
    #     return dataset_map

    # def init_dashboard_tiles(workspace: PowerBiReportServerAPI.Workspace) -> None:
    #     for dashboard in workspace.dashboards:
    #         dashboard.tiles = self.get_tiles(workspace, dashboard=dashboard)
    #
    #     return None
    #
    # LOGGER.info("Creating scan job for workspace")
    # LOGGER.info("{}={}".format(Constant.WorkspaceId, workspace_id))
    # LOGGER.info("Hitting URL={}".format(scan_create_endpoint))
    # scan_id = create_scan_job()
    # LOGGER.info("Waiting for scan to complete")
    # if (
    #     wait_for_scan_to_complete(
    #         scan_id=scan_id, timeout=self.__config.scan_timeout
    #     )
    #     is False
    # ):
    #     raise ValueError(
    #         "Workspace detail is not available. Please increase scan_timeout to wait."
    #     )
    #
    # # Scan is complete lets take the result
    # scan_result = get_scan_result(scan_id=scan_id)
    # workspace = PowerBiReportServerAPI.Workspace(
    #     id=scan_result["id"],
    #     name=scan_result["name"],
    #     state=scan_result["state"],
    #     datasets={},
    #     dashboards=[],
    # )
    # # Get workspace dashboards
    # workspace.dashboards = self.get_dashboards(workspace)
    # workspace.datasets = json_to_dataset_map(scan_result)
    # init_dashboard_tiles(workspace)
    #
    # return workspace


class Mapper:
    """
    Transfrom PowerBi concepts Dashboard, Dataset and Tile to DataHub concepts Dashboard, Dataset and Chart
    """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """
        We can add EquableMetadataWorkUnit to set.
        This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
        """

        def __eq__(self, instance):
            return self.id == self.id

        def __hash__(self):
            return id(self.id)

    def __init__(self, config: PowerBiDashboardSourceConfig):
        self.__config = config

    @staticmethod
    def new_mcp(
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
            aspect=aspect,
        )

    def __to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    # def __to_datahub_dataset(
    #     self, dataset: Optional[Dataset]
    # ) -> List[MetadataChangeProposalWrapper]:
    #     """
    #     Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
    #     In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.
    #     """
    #
    #     dataset_mcps: List[MetadataChangeProposalWrapper] = []
    #     if dataset is None:
    #         return dataset_mcps
    #
    #     # We are only suporting relation PowerBi DataSources
    #     if (
    #         dataset.datasource is None
    #         or dataset.datasource.metadata.is_relational is False
    #     ):
    #         LOGGER.warning(
    #             "Dataset {}({}) is not created from relational datasource".format(
    #                 dataset.name, dataset.id
    #             )
    #         )
    #         return dataset_mcps
    #
    #     LOGGER.info(
    #         "Converting dataset={}(id={}) to datahub dataset".format(
    #             dataset.name, dataset.id
    #         )
    #     )
    #
    #     for table in dataset.tables:
    #         # Create an URN for dataset
    #         ds_urn = builder.make_dataset_urn(
    #             platform=self.__config.dataset_type_mapping[dataset.datasource.type],
    #             name="{}.{}.{}".format(
    #                 dataset.datasource.database, table.schema_name, table.name
    #             ),
    #             env=self.__config.env,
    #         )
    #         LOGGER.info("{}={}".format(Constant.Dataset_URN, ds_urn))
    #         # Create datasetProperties mcp
    #         ds_properties = DatasetPropertiesClass(description=table.name)
    #
    #         info_mcp = self.new_mcp(
    #             entity_type=Constant.DATASET,
    #             entity_urn=ds_urn,
    #             aspect_name=Constant.DATASET_PROPERTIES,
    #             aspect=ds_properties,
    #         )
    #
    #         # Remove status mcp
    #         status_mcp = self.new_mcp(
    #             entity_type=Constant.DATASET,
    #             entity_urn=ds_urn,
    #             aspect_name=Constant.STATUS,
    #             aspect=StatusClass(removed=False),
    #         )
    #
    #         dataset_mcps.extend([info_mcp, status_mcp])
    #
    #     return dataset_mcps
    #
    # def __to_datahub_chart(
    #     self,
    #     tile: Tile,
    #     ds_mcps: List[MetadataChangeProposalWrapper],
    # ) -> List[MetadataChangeProposalWrapper]:
    #     """
    #     Map PowerBi tile to datahub chart
    #     """
    #     LOGGER.info("Converting tile {}(id={}) to chart".format(tile.title, tile.id))
    #     # Create an URN for chart
    #     chart_urn = builder.make_chart_urn(
    #         self.__config.platform_name, tile.get_urn_part()
    #     )
    #
    #     LOGGER.info("{}={}".format(Constant.CHART_URN, chart_urn))
    #
    #     ds_input: List[str] = self.to_urn_set(ds_mcps)
    #
    #     def tile_custom_properties(tile: Tile) -> dict:
    #         custom_properties = {
    #             "datasetId": tile.dataset.id if tile.dataset else "",
    #             "reportId": tile.report.id if tile.report else "",
    #             "datasetWebUrl": tile.dataset.webUrl
    #             if tile.dataset is not None
    #             else "",
    #             "createdFrom": tile.createdFrom.value,
    #         }
    #
    #         return custom_properties
    #
    #     # Create chartInfo mcp
    #     # Set chartUrl only if tile is created from Report
    #     chart_info_instance = ChartInfoClass(
    #         title=tile.title or "",
    #         description=tile.title or "",
    #         lastModified=ChangeAuditStamps(),
    #         inputs=ds_input,
    #         externalUrl=tile.report.webUrl if tile.report else None,
    #         customProperties={**tile_custom_properties(tile)},
    #     )
    #
    #     info_mcp = self.new_mcp(
    #         entity_type=Constant.CHART,
    #         entity_urn=chart_urn,
    #         aspect_name=Constant.CHART_INFO,
    #         aspect=chart_info_instance,
    #     )
    #
    #     # removed status mcp
    #     status_mcp = self.new_mcp(
    #         entity_type=Constant.CHART,
    #         entity_urn=chart_urn,
    #         aspect_name=Constant.STATUS,
    #         aspect=StatusClass(removed=False),
    #     )
    #
    #     # ChartKey status
    #     chart_key_instance = ChartKeyClass(
    #         dashboardTool=self.__config.platform_name,
    #         chartId=Constant.CHART_ID.format(tile.id),
    #     )
    #
    #     chartkey_mcp = self.new_mcp(
    #         entity_type=Constant.CHART,
    #         entity_urn=chart_urn,
    #         aspect_name=Constant.CHART_KEY,
    #         aspect=chart_key_instance,
    #     )
    #
    #     return [info_mcp, status_mcp, chartkey_mcp]
    #
    # # written in this style to fix linter error
    # def to_urn_set(self, mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    #     return list(
    #         OrderedSet(
    #             [
    #                 mcp.entityUrn
    #                 for mcp in mcps
    #                 if mcp is not None and mcp.entityUrn is not None
    #             ]
    #         )
    #     )
    #
    # def __to_datahub_dashboard(
    #     self,
    #     dashboard: Dashboard,
    #     chart_mcps: List[MetadataChangeProposalWrapper],
    #     user_mcps: List[MetadataChangeProposalWrapper],
    # ) -> List[MetadataChangeProposalWrapper]:
    #     """
    #     Map PowerBi dashboard to Datahub dashboard
    #     """
    #
    #     dashboard_urn = builder.make_dashboard_urn(
    #         self.__config.platform_name, dashboard.get_urn_part()
    #     )
    #
    #     chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
    #     user_urn_list: List[str] = self.to_urn_set(user_mcps)
    #
    #     def chart_custom_properties(
    #         dashboard: Dashboard,
    #     ) -> dict:
    #         return {
    #             "chartCount": str(len(dashboard.tiles)),
    #             "workspaceName": dashboard.workspace_name,
    #             "workspaceId": dashboard.id,
    #         }
    #
    #     # DashboardInfo mcp
    #     dashboard_info_cls = DashboardInfoClass(
    #         description=dashboard.displayName or "",
    #         title=dashboard.displayName or "",
    #         charts=chart_urn_list,
    #         lastModified=ChangeAuditStamps(),
    #         dashboardUrl=dashboard.webUrl,
    #         customProperties={**chart_custom_properties(dashboard)},
    #     )
    #
    #     info_mcp = self.new_mcp(
    #         entity_type=Constant.DASHBOARD,
    #         entity_urn=dashboard_urn,
    #         aspect_name=Constant.DASHBOARD_INFO,
    #         aspect=dashboard_info_cls,
    #     )
    #
    #     # removed status mcp
    #     removed_status_mcp = self.new_mcp(
    #         entity_type=Constant.DASHBOARD,
    #         entity_urn=dashboard_urn,
    #         aspect_name=Constant.STATUS,
    #         aspect=StatusClass(removed=False),
    #     )
    #
    #     # dashboardKey mcp
    #     dashboard_key_cls = DashboardKeyClass(
    #         dashboardTool=self.__config.platform_name,
    #         dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
    #     )
    #
    #     # Dashboard key
    #     dashboard_key_mcp = self.new_mcp(
    #         entity_type=Constant.DASHBOARD,
    #         entity_urn=dashboard_urn,
    #         aspect_name=Constant.DASHBOARD_KEY,
    #         aspect=dashboard_key_cls,
    #     )
    #
    #     # Dashboard Ownership
    #     owners = [
    #         OwnerClass(owner=user_urn, type=OwnershipTypeClass.CONSUMER)
    #         for user_urn in user_urn_list
    #         if user_urn is not None
    #     ]
    #     ownership = OwnershipClass(owners=owners)
    #     # Dashboard owner MCP
    #     owner_mcp = self.new_mcp(
    #         entity_type=Constant.DASHBOARD,
    #         entity_urn=dashboard_urn,
    #         aspect_name=Constant.OWNERSHIP,
    #         aspect=ownership,
    #     )
    #
    #     # Dashboard browsePaths
    #     browse_path = BrowsePathsClass(
    #         paths=["/powerbi/{}".format(self.__config.workspace_id)]
    #     )
    #     browse_path_mcp = self.new_mcp(
    #         entity_type=Constant.DASHBOARD,
    #         entity_urn=dashboard_urn,
    #         aspect_name=Constant.BROWSERPATH,
    #         aspect=browse_path,
    #     )
    #
    #     return [
    #         browse_path_mcp,
    #         info_mcp,
    #         removed_status_mcp,
    #         dashboard_key_mcp,
    #         owner_mcp,
    #     ]

    def to_datahub_user(
        self, user: SystemPolicies
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBiReportServer user to datahub user
        """

        LOGGER.info(
            "Converting user {}(id={}) to datahub's user".format(
                user.GroupUserName, user.
            )
        )

        # Create an URN for user
        user_urn = builder.make_user_urn(user.get_urn_part())

        user_info_instance = CorpUserInfoClass(
            displayName=user.displayName,
            email=user.emailAddress,
            title=user.displayName,
            active=True,
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_INFO,
            aspect=user_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        user_key = CorpUserKeyClass(username=user.id)

        user_key_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_KEY,
            aspect=user_key,
        )

        return [info_mcp, status_mcp, user_key_mcp]
    #
    def to_datahub_users(
        self, users: List[SystemPolicies]
    ) -> List[MetadataChangeProposalWrapper]:
        user_mcps = []

        for user in users:
            user_mcps.extend(self.to_datahub_user(user))

        return user_mcps

    # def to_datahub_chart(
    #     self, tiles: List[PowerBiReportServerAPI.Tile]
    # ) -> Tuple[
    #     List[MetadataChangeProposalWrapper], List[MetadataChangeProposalWrapper]
    # ]:
    #     ds_mcps = []
    #     chart_mcps = []
    #
    #     # Return empty list if input list is empty
    #     if len(tiles) == 0:
    #         return [], []
    #
    #     LOGGER.info("Converting tiles(count={}) to charts".format(len(tiles)))
    #
    #     for tile in tiles:
    #         if tile is None:
    #             continue
    #         # First convert the dataset to MCP, because dataset mcp is used in input attribute of chart mcp
    #         dataset_mcps = self.__to_datahub_dataset(tile.dataset)
    #         # Now convert tile to chart MCP
    #         chart_mcp = self.__to_datahub_chart(tile, dataset_mcps)
    #
    #         ds_mcps.extend(dataset_mcps)
    #         chart_mcps.extend(chart_mcp)
    #
    #     # Return dataset and chart MCPs
    #
    #     return ds_mcps, chart_mcps
    # TODO: FIX
    # def to_datahub_work_units(
    #     self, report: Report
    # ) -> Set[EquableMetadataWorkUnit]:
    #     mcps = []
    #
    #     LOGGER.info(
    #         "Converting dashboard={} to datahub dashboard".format(report.Name)
    #     )
    #
    #     # Convert user to CorpUser
    #     user_mcps = self.to_datahub_users(dashboard.users)
    #     # Convert tiles to charts
    #     ds_mcps, chart_mcps = self.to_datahub_chart(dashboard.tiles)
    #     # Lets convert dashboard to datahub dashboard
    #     dashboard_mcps = self.__to_datahub_dashboard(dashboard, chart_mcps, user_mcps)
    #
    #     # Now add MCPs in sequence
    #     mcps.extend(ds_mcps)
    #     mcps.extend(user_mcps)
    #     mcps.extend(chart_mcps)
    #     mcps.extend(dashboard_mcps)
    #
    #     # Convert MCP to work_units
    #     work_units = map(self.__to_work_unit, mcps)
    #     # Return set of work_unit
    #     return OrderedSet([wu for wu in work_units if wu is not None])


@dataclass
class PowerBiReportServerDashboardSourceReport(SourceReport):
    scanned_report: int = 0
    # charts_scanned: int = 0
    # filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_reports: List[str] = dataclass_field(default_factory=list)

    def report_scanned(self, count: int = 1) -> None:
        self.scanned_report += count

    # def report_charts_scanned(self, count: int = 1) -> None:
    #     self.charts_scanned += count
    #
    # def report_dashboards_dropped(self, model: str) -> None:
    #     self.filtered_dashboards.append(model)

    def report_dropped(self, view: str) -> None:
        self.filtered_reports.append(view)


@platform_name("PowerBIReportServer")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class PowerBiReportServerDashboardSource(Source):
    """
        This plugin extracts the following:

    - Power BI dashboards, tiles, datasets
    - Names, descriptions and URLs of dashboard and tile
    - Owners of dashboards

    ## Configuration Notes

    See the
    1.  [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) for the steps to create a app client ID and secret.
    2.  Login to Power BI as Admin and from `Tenant settings` allow below permissions.
        - Allow service principles to use Power BI APIs
        - Allow service principals to use read-only Power BI admin APIs
        - Enhance admin APIs responses with detailed metadata
    """

    source_config: PowerBiDashboardSourceConfig
    reporter: PowerBiReportServerDashboardSourceReport
    accessed_dashboards: int = 0

    def __init__(self, config: PowerBiDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = PowerBiReportServerDashboardSourceReport()
        self.auth = PowerBiReportServerAPI(self.source_config).get_auth_credentials()
        self.powerbi_client = PowerBiReportServerAPI(self.source_config)
        self.mapper = Mapper(config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    # TODO: FIX
    # def get_workunits(self) -> Iterable[MetadataWorkUnit]:
    #     """
    #     Datahub Ingestion framework invoke this method
    #     """
    #     LOGGER.info("PowerBiReportServer plugin execution is started")
    #
    #     # Fetch PowerBiReportServer reports for given url
    #     # workspace = self.powerbi_client.get_workspace(self.source_config.workspace_id)
    #     reports = self.powerbi_client.get_reports
    #
    #     for report in reports:
    #         try:
    #             # Fetch PowerBi users for dashboards
    #             dashboard.users = self.powerbi_client.get_dashboard_users(dashboard)
    #             # Increase dashboard and tiles count in report
    #             self.reporter.report_dashboards_scanned()
    #             self.reporter.report_charts_scanned(count=len(dashboard.tiles))
    #         except Exception as e:
    #             message = "Error ({}) occurred while loading dashboard {}(id={}) tiles.".format(
    #                 e, report.Name, report.Id
    #             )
    #             LOGGER.exception(message, e)
    #             self.reporter.report_warning(report.Id, message)
    #
    #         # Convert PowerBi Dashboard and child entities to Datahub work unit to ingest into Datahub
    #         workunits = self.mapper.to_datahub_work_units(dashboard)
    #         for workunit in workunits:
    #             # Add workunit to report
    #             self.reporter.report_workunit(workunit)
    #             # Return workunit to Datahub Ingestion framework
    #             yield workunit

    def get_report(self) -> SourceReport:
        return self.reporter
