# Outputs for ECR infrastructure
# Provides repository URLs and OIDC role ARN for GitHub Actions

output "ecr_prod_repo" {
  description = "Production ECR repository URL"
  value       = aws_ecr_repository.prod.repository_url
}

output "ecr_dev_repo" {
  description = "Development ECR repository URL"
  value       = aws_ecr_repository.dev.repository_url
}

output "ecr_prod_name" {
  description = "Production ECR repository name"
  value       = aws_ecr_repository.prod.name
}

output "ecr_dev_name" {
  description = "Development ECR repository name"
  value       = aws_ecr_repository.dev.name
}

output "project_name" {
  description = "Detected project name"
  value       = local.project_name
}

output "github_oidc_role_arn" {
  description = "GitHub OIDC IAM role ARN (if created)"
  value       = var.create_oidc_role ? aws_iam_role.github_oidc[0].arn : null
}

output "aws_region" {
  description = "AWS region for the ECR repositories"
  value       = var.aws_region
}
