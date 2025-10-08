# ECR Infrastructure for closurebot projects
# Creates two ECR repositories per project: -prod and -dev
# Applies lifecycle policies to automatically clean up old images

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

# Auto-detect project name from GitHub repository if not provided
locals {
  # Try to get GitHub repo from environment variable, fall back to variable
  github_repo = var.github_repo != "" ? var.github_repo : (try(getenv("GITHUB_REPOSITORY"), ""))
  
  # Extract project name from GitHub repo (e.g., "username/closurebot-on511" -> "closurebot-on511")
  # If no repo provided, use a default based on directory name
  detected_project = var.project_name != "" ? var.project_name : (
    local.github_repo != "" ? split("/", local.github_repo)[1] : "closurebot-on511"
  )
  project_name     = local.detected_project
}

# ECR Repository for Production (master branch deployments)
resource "aws_ecr_repository" "prod" {
  name                 = "${local.project_name}-prod"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name        = "${local.project_name}-prod"
    Environment = "production"
    Project     = local.project_name
  }
}

# ECR Repository for Development (develop branch deployments)
resource "aws_ecr_repository" "dev" {
  name                 = "${local.project_name}-dev"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name        = "${local.project_name}-dev"
    Environment = "development"
    Project     = local.project_name
  }
}

# Lifecycle policy for Production ECR repository
# - Keeps last 2 latest-* images for rollback
# - Deletes untagged images after 1 day
# - Deletes commit-tagged images older than 2 days
resource "aws_ecr_lifecycle_policy" "prod" {
  repository = aws_ecr_repository.prod.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 2 latest-* images for rollback"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["latest-"]
          countType     = "imageCountMoreThan"
          countNumber   = 2
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Delete untagged images after 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 3
        description  = "Delete commit-tagged images older than 2 days"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["closurebot-"]
          countType     = "sinceImagePushed"
          countUnit     = "days"
          countNumber   = 2
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Lifecycle policy for Development ECR repository
# - Same policy as production for consistency
resource "aws_ecr_lifecycle_policy" "dev" {
  repository = aws_ecr_repository.dev.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 2 latest-* images for rollback"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["latest-"]
          countType     = "imageCountMoreThan"
          countNumber   = 2
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Delete untagged images after 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 3
        description  = "Delete commit-tagged images older than 2 days"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["closurebot-"]
          countType     = "sinceImagePushed"
          countUnit     = "days"
          countNumber   = 2
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Optional: GitHub OIDC IAM Role for this specific project
# Only created if create_oidc_role is true
resource "aws_iam_role" "github_oidc" {
  count = var.create_oidc_role ? 1 : 0
  name  = "${local.project_name}-github-oidc"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRoleWithWebIdentity"
        Effect = "Allow"
        Principal = {
          Federated = var.github_oidc_provider_arn
        }
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:*"
          }
        }
      }
    ]
  })

  tags = {
    Name    = "${local.project_name}-github-oidc"
    Project = local.project_name
  }
}

# IAM policy for ECR access
resource "aws_iam_policy" "ecr_access" {
  count = var.create_oidc_role ? 1 : 0
  name  = "${local.project_name}-ecr-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
          "ecr:BatchDeleteImage"
        ]
        Resource = [
          aws_ecr_repository.prod.arn,
          aws_ecr_repository.dev.arn
        ]
      }
    ]
  })
}

# Attach ECR policy to GitHub OIDC role
resource "aws_iam_role_policy_attachment" "github_oidc_ecr" {
  count      = var.create_oidc_role ? 1 : 0
  role       = aws_iam_role.github_oidc[0].name
  policy_arn = aws_iam_policy.ecr_access[0].arn
}
