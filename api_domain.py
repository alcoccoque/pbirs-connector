from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel


class CreatedFrom(Enum):
    REPORT = "Report"
    DATASET = "Dataset"
    VISUALIZATION = "Visualization"
    UNKNOWN = "UNKNOWN"


class CatalogItem(BaseModel):
    Id: str
    Name: str
    Description: Optional[str]
    Path: str
    Type: Any
    Hidden: bool
    Size: int
    ModifiedBy: str
    ModifiedDate: datetime
    CreatedBy: str
    CreatedDate: datetime
    ParentFolderId: str
    ContentType: Optional[str]
    Content: str
    IsFavorite: bool


class DataSet(CatalogItem):
    HasParameters: bool
    QueryExecutionTimeOut: int

    def get_urn_part(self):
        return "datasets.{}".format(self.Id)

    def __members(self):
        return (self.Id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSet) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class DataModelDataSource(BaseModel):
    AuthType: Optional[str]
    SupportedAuthTypes: List[Optional[str]]
    Kind: Optional[Callable]
    ModelConnectionName: str
    Secret: str
    Type: Optional[str]
    Username: str


class CredentialsByUser(BaseModel):
    DisplayText: str
    UseAsWindowsCredentials: bool


class CredentialsInServer(BaseModel):
    UserName: str
    Password: str
    UseAsWindowsCredentials: bool
    ImpersonateAuthenticatedUser: bool


class ParameterValue(BaseModel):
    Name: str
    Value: str
    IsValueFieldReference: str


class ExtensionSettings(BaseModel):
    Extension: str
    ParameterValues: ParameterValue


class Subscription(BaseModel):
    Id: str
    Owner: str
    IsDataDriven: bool
    Description: str
    Report: str
    IsActive: bool
    EventType: str
    ScheduleDescription: str
    LastRunTime: datetime
    LastStatus: str
    ExtensionSettings: ExtensionSettings
    DeliveryExtension: str
    LocalizedDeliveryExtensionName: str
    ModifiedBy: str
    ModifiedDate: datetime
    ParameterValues: ParameterValue


class MetaData(BaseModel):
    is_relational: bool


class DataSource(CatalogItem):
    IsEnabled: bool
    DataModelDataSource: DataModelDataSource
    DataSourceSubType: str
    DataSourceType: str
    IsOriginalConnectionStringExpressionBased: bool
    IsConnectionStringOverridden: bool
    CredentialsByUser: CredentialsByUser
    CredentialsInServer: CredentialsInServer
    IsReference: bool
    Subscriptions: Subscription
    MetaData: MetaData

    def __members(self):
        return (self.Id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSource)
            and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class Comment(BaseModel):
    Id: str
    ItemId: str
    UserName: str
    ThreadId: str
    AttachmentPath: str
    Text: str
    CreatedDate: datetime
    ModifiedDate: datetime


class ExcelWorkbook(CatalogItem):
    Comments: Comment


class Report(BaseModel):
    Id: str
    Name: str
    Description: Optional[str]
    Path: str
    Type: Any
    Hidden: bool
    Size: int
    ModifiedBy: str
    ModifiedDate: datetime
    CreatedBy: str
    CreatedDate: datetime
    ParentFolderId: str
    ContentType: Optional[str]
    Content: str
    IsFavorite: bool
    HasDataSources: bool
    HasSharedDataSets: bool
    HasParameters: bool

    def get_urn_part(self):
        return "reports.{}".format(self.Id)


class Extension(BaseModel):
    ExtensionType: str
    Name: str
    LocalizedName: str
    Visible: bool


class Folder(CatalogItem):
    """Folder"""


class DrillThroughTarget(BaseModel):
    DrillThroughTargetType: str


class Value(BaseModel):
    Value: str
    Goal: int
    Status: int
    TrendSet: List[int]


class Kpi(CatalogItem):
    ValuerFormat: str
    Visualization: str
    DrillThroughTarget: DrillThroughTarget
    Currency: str
    Values: Value
    Data: Dict[str, str]


class LinkedReport(CatalogItem):
    HasParemeters: bool
    Link: str


class Manifest(BaseModel):
    Resorces: List[Dict[str, List]]


class MobileReport(CatalogItem):
    AllowCaching: bool
    Manifest: Manifest


class PowerBIReport(CatalogItem):
    HasDataSources: bool


class Resources(CatalogItem):
    """Resources"""


class System(BaseModel):
    ReportServerAbsoluteUrl: str
    ReportServerRelativeUrl: str
    WebPortalRelativeUrl: str
    ProductName: str
    ProductVersion: str
    ProductType: str
    TimeZone: str


# class Dashboard(BaseModel):
#     id: str
#     displayName: str
#     embedUrl: str
#     webUrl: str
#     isReadOnly: Any
#     workspace_id: str
#     workspace_name: str
#     tiles: List[Any]
#     users: List[Any]
#
#     def get_urn_part(self):
#         return "dashboards.{}".format(self.id)
#
#     def __members(self):
#         return (self.id,)
#
#     def __eq__(self, instance):
#         return (
#                 isinstance(instance, Dashboard)
#                 and self.__members() == instance.__members()
#         )
#
#     def __hash__(self):
#         return hash(self.__members())
