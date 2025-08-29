# MLB Betting System - Production Infrastructure
# Terraform configuration for production-ready infrastructure
# Implements high availability, security, monitoring, and disaster recovery

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
  
  backend "s3" {
    bucket = "mlb-betting-terraform-state"
    key    = "production/terraform.tfstate"
    region = "us-west-2"
    
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "mlb-betting-system"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "devops-team"
      CostCenter  = "engineering"
    }
  }
}

# ============================================================================
# DATA SOURCES
# ============================================================================

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# ============================================================================
# LOCAL VALUES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    CreatedAt   = timestamp()
  }
  
  # Availability zones (use first 3 available)
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
  
  # Database configuration
  db_port = 5432
  redis_port = 6379
  
  # Application ports
  app_port = 8000
  monitoring_port = 8001
  mlflow_port = 5001
}

# ============================================================================
# NETWORKING
# ============================================================================

module "vpc" {
  source = "./modules/vpc"
  
  name_prefix = local.name_prefix
  
  # CIDR configuration
  vpc_cidr             = var.vpc_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
  database_subnet_cidrs = var.database_subnet_cidrs
  
  availability_zones = local.azs
  
  # NAT Gateway configuration
  enable_nat_gateway = true
  single_nat_gateway = var.environment == "staging" ? true : false
  
  # VPC Flow Logs
  enable_flow_log = true
  flow_log_destination_type = "s3"
  flow_log_s3_bucket_arn = module.s3_buckets.flow_logs_bucket_arn
  
  # DNS configuration
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = local.common_tags
}

# ============================================================================
# SECURITY GROUPS
# ============================================================================

# Application Load Balancer Security Group
resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP traffic"
  }
  
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS traffic"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-alb-sg"
  })
}

# ECS Service Security Group
resource "aws_security_group" "ecs_service" {
  name_prefix = "${local.name_prefix}-ecs-"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port       = local.app_port
    to_port         = local.app_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
    description     = "HTTP from ALB"
  }
  
  ingress {
    from_port       = local.monitoring_port
    to_port         = local.monitoring_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
    description     = "Monitoring from ALB"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ecs-sg"
  })
}

# Database Security Group
resource "aws_security_group" "database" {
  name_prefix = "${local.name_prefix}-db-"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port       = local.db_port
    to_port         = local.db_port
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_service.id]
    description     = "PostgreSQL from ECS services"
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-db-sg"
  })
}

# Redis Security Group
resource "aws_security_group" "redis" {
  name_prefix = "${local.name_prefix}-redis-"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port       = local.redis_port
    to_port         = local.redis_port
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_service.id]
    description     = "Redis from ECS services"
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-redis-sg"
  })
}

# ============================================================================
# S3 BUCKETS
# ============================================================================

module "s3_buckets" {
  source = "./modules/s3"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # Bucket configurations
  create_artifacts_bucket   = true
  create_backups_bucket    = true
  create_logs_bucket       = true
  create_flow_logs_bucket  = true
  
  # Security configurations
  enable_versioning = true
  enable_encryption = true
  
  # Lifecycle configurations
  artifacts_lifecycle_days = 90
  backups_lifecycle_days   = 365
  logs_lifecycle_days      = 30
  
  tags = local.common_tags
}

# ============================================================================
# DATABASE (RDS PostgreSQL)
# ============================================================================

module "database" {
  source = "./modules/rds"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # Database configuration
  engine_version    = "15.4"
  instance_class   = var.db_instance_class
  allocated_storage = var.db_allocated_storage
  max_allocated_storage = var.db_max_allocated_storage
  
  # Database credentials
  db_name  = "mlb_betting"
  username = "mlb_admin"
  
  # Network configuration
  vpc_id               = module.vpc.vpc_id
  subnet_ids          = module.vpc.database_subnet_ids
  vpc_security_group_ids = [aws_security_group.database.id]
  
  # High availability
  multi_az               = var.environment == "production" ? true : false
  backup_retention_period = var.environment == "production" ? 30 : 7
  backup_window         = "03:00-04:00"
  maintenance_window    = "sun:04:00-sun:05:00"
  
  # Performance Insights
  performance_insights_enabled = true
  performance_insights_retention_period = 7
  
  # Monitoring
  monitoring_interval = 60
  monitoring_role_arn = aws_iam_role.rds_enhanced_monitoring.arn
  
  # Encryption
  storage_encrypted = true
  kms_key_id       = aws_kms_key.main.arn
  
  # Deletion protection
  deletion_protection = var.environment == "production" ? true : false
  skip_final_snapshot = var.environment == "production" ? false : true
  
  tags = local.common_tags
}

# ============================================================================
# REDIS (ElastiCache)
# ============================================================================

module "redis" {
  source = "./modules/elasticache"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # Redis configuration
  node_type           = var.redis_node_type
  num_cache_nodes     = var.environment == "production" ? 3 : 1
  engine_version      = "7.0"
  parameter_group_name = "default.redis7"
  port               = local.redis_port
  
  # Network configuration
  subnet_group_name = aws_elasticache_subnet_group.main.name
  security_group_ids = [aws_security_group.redis.id]
  
  # High availability
  automatic_failover_enabled = var.environment == "production" ? true : false
  multi_az_enabled          = var.environment == "production" ? true : false
  
  # Backup configuration
  snapshot_retention_limit = var.environment == "production" ? 5 : 1
  snapshot_window         = "03:00-05:00"
  
  # Encryption
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                = random_password.redis_auth.result
  
  tags = local.common_tags
}

resource "aws_elasticache_subnet_group" "main" {
  name       = "${local.name_prefix}-cache-subnet"
  subnet_ids = module.vpc.private_subnet_ids
  
  tags = local.common_tags
}

resource "random_password" "redis_auth" {
  length  = 32
  special = true
}

# ============================================================================
# ECS CLUSTER AND SERVICES
# ============================================================================

module "ecs" {
  source = "./modules/ecs"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # Cluster configuration
  cluster_name = "${local.name_prefix}-cluster"
  
  # Service configuration
  services = {
    fastapi = {
      task_definition_family = "${local.name_prefix}-fastapi"
      container_definitions = [
        {
          name  = "fastapi"
          image = var.fastapi_image
          port  = local.app_port
          environment_variables = {
            ENVIRONMENT    = var.environment
            DATABASE_URL   = "postgresql://${module.database.username}:${module.database.password}@${module.database.endpoint}:${module.database.port}/${module.database.db_name}"
            REDIS_URL      = "rediss://:${random_password.redis_auth.result}@${module.redis.primary_endpoint}:${local.redis_port}"
            MLFLOW_TRACKING_URI = "http://${aws_lb.main.dns_name}:${local.mlflow_port}"
          }
          secrets = {
            DATABASE_PASSWORD = aws_ssm_parameter.db_password.arn
            REDIS_AUTH_TOKEN = aws_ssm_parameter.redis_auth.arn
          }
        }
      ]
      desired_count = var.fastapi_desired_count
      cpu          = var.fastapi_cpu
      memory       = var.fastapi_memory
    }
    
    monitoring = {
      task_definition_family = "${local.name_prefix}-monitoring"
      container_definitions = [
        {
          name  = "monitoring-dashboard"
          image = var.monitoring_image
          port  = local.monitoring_port
          environment_variables = {
            ENVIRONMENT = var.environment
          }
        }
      ]
      desired_count = 1
      cpu          = 512
      memory       = 1024
    }
  }
  
  # Network configuration
  vpc_id            = module.vpc.vpc_id
  subnet_ids        = module.vpc.private_subnet_ids
  security_group_ids = [aws_security_group.ecs_service.id]
  
  # Load balancer integration
  load_balancer_arn         = aws_lb.main.arn
  load_balancer_listener_arn = aws_lb_listener.https.arn
  
  tags = local.common_tags
}

# ============================================================================
# APPLICATION LOAD BALANCER
# ============================================================================

resource "aws_lb" "main" {
  name               = "${local.name_prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets           = module.vpc.public_subnet_ids
  
  enable_deletion_protection = var.environment == "production" ? true : false
  
  # Access logs
  access_logs {
    bucket  = module.s3_buckets.logs_bucket_name
    prefix  = "alb-access-logs"
    enabled = true
  }
  
  tags = local.common_tags
}

# HTTPS Listener
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS-1-2-2017-01"
  certificate_arn   = aws_acm_certificate_validation.main.certificate_arn
  
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.fastapi.arn
  }
}

# HTTP Listener (redirect to HTTPS)
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = "80"
  protocol          = "HTTP"
  
  default_action {
    type = "redirect"
    
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

# Target Groups
resource "aws_lb_target_group" "fastapi" {
  name        = "${local.name_prefix}-fastapi"
  port        = local.app_port
  protocol    = "HTTP"
  vpc_id      = module.vpc.vpc_id
  target_type = "ip"
  
  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 30
    matcher             = "200"
    path                = "/health"
    port                = "traffic-port"
    protocol            = "HTTP"
    timeout             = 5
    unhealthy_threshold = 2
  }
  
  tags = local.common_tags
}

resource "aws_lb_target_group" "monitoring" {
  name        = "${local.name_prefix}-monitoring"
  port        = local.monitoring_port
  protocol    = "HTTP"
  vpc_id      = module.vpc.vpc_id
  target_type = "ip"
  
  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 30
    matcher             = "200"
    path                = "/api/health"
    port                = "traffic-port"
    protocol            = "HTTP"
    timeout             = 5
    unhealthy_threshold = 2
  }
  
  tags = local.common_tags
}

# ============================================================================
# SSL CERTIFICATE
# ============================================================================

resource "aws_acm_certificate" "main" {
  domain_name       = var.domain_name
  validation_method = "DNS"
  
  subject_alternative_names = [
    "*.${var.domain_name}"
  ]
  
  lifecycle {
    create_before_destroy = true
  }
  
  tags = local.common_tags
}

resource "aws_acm_certificate_validation" "main" {
  certificate_arn         = aws_acm_certificate.main.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
  
  timeouts {
    create = "5m"
  }
}

# ============================================================================
# ROUTE 53
# ============================================================================

data "aws_route53_zone" "main" {
  name         = var.domain_name
  private_zone = false
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.main.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }
  
  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = data.aws_route53_zone.main.zone_id
}

resource "aws_route53_record" "main" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = var.domain_name
  type    = "A"
  
  alias {
    name                   = aws_lb.main.dns_name
    zone_id                = aws_lb.main.zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "monitoring" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = "monitoring.${var.domain_name}"
  type    = "A"
  
  alias {
    name                   = aws_lb.main.dns_name
    zone_id                = aws_lb.main.zone_id
    evaluate_target_health = true
  }
}

# ============================================================================
# KMS KEY
# ============================================================================

resource "aws_kms_key" "main" {
  description             = "KMS key for ${local.name_prefix}"
  deletion_window_in_days = var.environment == "production" ? 30 : 7
  enable_key_rotation     = true
  
  tags = local.common_tags
}

resource "aws_kms_alias" "main" {
  name          = "alias/${local.name_prefix}"
  target_key_id = aws_kms_key.main.key_id
}

# ============================================================================
# SECRETS MANAGER
# ============================================================================

resource "aws_ssm_parameter" "db_password" {
  name  = "/${local.name_prefix}/database/password"
  type  = "SecureString"
  value = module.database.password
  
  tags = local.common_tags
}

resource "aws_ssm_parameter" "redis_auth" {
  name  = "/${local.name_prefix}/redis/auth-token"
  type  = "SecureString"
  value = random_password.redis_auth.result
  
  tags = local.common_tags
}

# ============================================================================
# IAM ROLES
# ============================================================================

# RDS Enhanced Monitoring Role
resource "aws_iam_role" "rds_enhanced_monitoring" {
  name = "${local.name_prefix}-rds-monitoring-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "rds_enhanced_monitoring" {
  role       = aws_iam_role.rds_enhanced_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# ============================================================================
# MONITORING AND ALERTING
# ============================================================================

module "monitoring" {
  source = "./modules/monitoring"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # CloudWatch configuration
  create_dashboards = true
  create_alarms     = true
  
  # SNS topic for alerts
  sns_topic_name = "${local.name_prefix}-alerts"
  alert_email    = var.alert_email
  
  # Monitoring targets
  database_identifier = module.database.db_instance_id
  redis_cluster_id   = module.redis.cluster_id
  load_balancer_arn_suffix = aws_lb.main.arn_suffix
  target_group_arn_suffix  = aws_lb_target_group.fastapi.arn_suffix
  
  tags = local.common_tags
}

# ============================================================================
# AUTO SCALING
# ============================================================================

module "autoscaling" {
  source = "./modules/autoscaling"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # ECS service autoscaling
  ecs_cluster_name = module.ecs.cluster_name
  ecs_service_name = module.ecs.service_names["fastapi"]
  
  # Scaling configuration
  min_capacity = var.environment == "production" ? 2 : 1
  max_capacity = var.environment == "production" ? 10 : 3
  
  # Scaling policies
  scale_up_threshold   = 70  # CPU percentage
  scale_down_threshold = 30  # CPU percentage
  
  tags = local.common_tags
}

# ============================================================================
# BACKUP AND DISASTER RECOVERY
# ============================================================================

module "backup" {
  source = "./modules/backup"
  
  name_prefix = local.name_prefix
  environment = var.environment
  
  # Database backups
  db_instance_id = module.database.db_instance_id
  
  # S3 bucket backups
  s3_bucket_names = [
    module.s3_buckets.artifacts_bucket_name,
    module.s3_buckets.logs_bucket_name
  ]
  
  # Backup schedule
  backup_schedule = "cron(0 2 * * ? *)"  # Daily at 2 AM
  backup_retention_days = var.environment == "production" ? 30 : 7
  
  # Cross-region backup
  enable_cross_region_backup = var.environment == "production" ? true : false
  backup_destination_region  = "us-east-1"
  
  tags = local.common_tags
}