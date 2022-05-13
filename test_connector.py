from powerbireportserver.powerbi import PowerBiDashboardSource
from powerbireportserver.powerbi_report_server import (
    PowerBiReportServerAPI,
    PowerBiReportServerAPIConfig,
)

conf_dict = dict(
    username="",
    password="",
    workstation_name="desktop-r0v279t",
    report_virtual_directory_name="Reports",
    report_server_virtual_directory_name="ReportServer",
    dataset_type_mapping={},
)
config = PowerBiReportServerAPIConfig.parse_obj(conf_dict)
pbirs_api = PowerBiReportServerAPI(config)
dataset = pbirs_api.get_dataset("a0874ae4-a988-44e9-8217-cebef8893493")
print(pbirs_api.get_data_source(dataset))
# dashboard = PowerBiAPIConfig.parse_obj(conf)
