# S1: the Redshift warehouse (infra/modules/redshift). Clients use the Data API, so there is no
# ingress to open; COPY and UNLOAD use the LabRole the module attaches as the default role.

module "redshift" {
  source     = "../modules/redshift"
  subnet_ids = data.aws_subnets.all.ids
  # Same AZ as the hosts (main.tf), so COPY/UNLOAD and Data API traffic stays in-AZ.
  availability_zone = var.availability_zone
}

# Outputs listed in contracts/platform.yaml under terraform_outputs.

output "redshift_cluster_id" {
  value = module.redshift.cluster_id
}

output "redshift_database" {
  value = module.redshift.database
}
