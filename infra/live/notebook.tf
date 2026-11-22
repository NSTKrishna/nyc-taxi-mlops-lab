# S2: wire infra/modules/notebook (the SageMaker notebook) into this root module, let it reach MLflow
# on the platform host (port 5000, runtime.yaml), and output its name as notebook_name.
# Set github_repo in terraform.tfvars: the notebook clones your public repo.
#
# Created only once github_repo is set, so plans before S2 don't need it.

module "notebook" {
  source    = "../modules/notebook"
  count     = var.github_repo == null ? 0 : 1
  subnet_id = data.aws_subnet.host.id # the hosts' AZ: MLflow traffic stays in-AZ
  repo_url  = "https://github.com/${var.github_repo}.git"
  env = {
    MLFLOW_TRACKING_URI = "http://${module.platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    REDSHIFT_DB_USER    = "labadmin" # contracts/platform.yaml: the notebook reads staging, which lab_ro can't
    LAB_ARTIFACT_BUCKET = module.artifact_bucket.name
  }
}

# notebook -> platform: MLflow (the module's own security group allows no ingress)
resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  count                        = var.github_repo == null ? 0 : 1
  security_group_id            = aws_security_group.platform.id
  referenced_security_group_id = module.notebook[0].security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  description                  = "MLflow from the notebook"
}

output "notebook_name" {
  value = one(module.notebook[*].name)
}
