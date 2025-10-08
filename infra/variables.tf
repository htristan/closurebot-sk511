# Variables for ECR infrastructure
# Auto-detects project name from GitHub repository name

variable "project_name" {
  description = "Name of the project (e.g., closurebot-on511). Auto-detected from GITHUB_REPOSITORY if not provided."
  type        = string
  default     = ""
}

variable "aws_region" {
  description = "AWS region for ECR repositories"
  type        = string
  default     = "us-east-1"
}

variable "github_repo" {
  description = "GitHub repository in format owner/repo (e.g., username/closurebot-on511). Auto-detected from GITHUB_REPOSITORY env var if not provided."
  type        = string
  default     = ""
}

variable "create_oidc_role" {
  description = "Whether to create a GitHub OIDC IAM role for this project"
  type        = bool
  default     = false
}

variable "github_oidc_provider_arn" {
  description = "ARN of the GitHub OIDC provider (if using shared OIDC setup)"
  type        = string
  default     = ""
}
