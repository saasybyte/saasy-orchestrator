use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct GcpConfig {
    pub project_id: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct AwsConfig {
    pub region: String,
}

pub struct CloudProviderStore {
    pub gcp: GcpConfig,
    pub aws: AwsConfig,
    // Future: pub azure: AzureConfig,
}

impl CloudProviderStore {
    pub fn new(configs: HashMap<String, String>) -> Self {
        Self {
            gcp: GcpConfig {
                project_id: configs.get("gcp_project_id").cloned().unwrap_or_default(),
                region: configs.get("gcp_region").cloned().unwrap_or_default(),
            },
            aws: AwsConfig {
                region: configs.get("aws_region").cloned().unwrap_or_default(),
            },
        }
    }
}
