# Infrastructure as Code

This directory contains Terraform configuration for the closurebot-on511 project's AWS infrastructure.

## What it creates

- **ECR Repositories**: Two ECR repos for container images
  - `closurebot-on511-prod`: Production images (master branch)
  - `closurebot-on511-dev`: Development images (develop branch)
- **Lifecycle Policies**: Automatic cleanup of old images
  - Keeps latest-* images safe
  - Deletes commit-tagged images older than 2 days
  - Deletes untagged images after 1 day
  - Keeps last 2 latest-* images for rollback

## Usage

### Initialize and Plan
```bash
cd infra
terraform init
terraform plan
```

### Apply Infrastructure
```bash
terraform apply
```

### Get Outputs
```bash
terraform output
```

## Variables

The configuration auto-detects the project name from the GitHub repository. You can override with:

```bash
terraform apply -var="project_name=closurebot-on511" -var="github_repo=username/closurebot-on511"
```

## Outputs

- `ecr_prod_repo`: Production ECR repository URL
- `ecr_dev_repo`: Development ECR repository URL
- `ecr_prod_name`: Production ECR repository name
- `ecr_dev_name`: Development ECR repository name
- `project_name`: Detected project name

## GitHub Actions Integration

The GitHub Actions workflow automatically:
- Pushes to `-prod` repo on master branch
- Pushes to `-dev` repo on develop branch
- Uses the same OIDC role for authentication
