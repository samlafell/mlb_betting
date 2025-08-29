# Variables for MLB Betting System Production Infrastructure

# ============================================================================
# GENERAL CONFIGURATION
# ============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "mlb-betting-system"
}

variable "environment" {
  description = "Environment name (production, staging, development)"
  type        = string
  default     = "production"
  
  validation {
    condition     = contains(["production", "staging", "development"], var.environment)
    error_message = "Environment must be one of: production, staging, development."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "domain_name" {
  description = "Domain name for the application"
  type        = string
  default     = "mlb-betting.example.com"
}

variable "alert_email" {
  description = "Email address for alerts and notifications"
  type        = string
  default     = "alerts@example.com"
}

# ============================================================================
# NETWORKING CONFIGURATION
# ============================================================================

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

variable "database_subnet_cidrs" {
  description = "CIDR blocks for database subnets"
  type        = list(string)
  default     = ["10.0.21.0/24", "10.0.22.0/24", "10.0.23.0/24"]
}

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.r6g.large"
  
  validation {
    condition = contains([
      "db.r6g.large", "db.r6g.xlarge", "db.r6g.2xlarge", 
      "db.r5.large", "db.r5.xlarge", "db.r5.2xlarge",
      "db.t3.medium", "db.t3.large"
    ], var.db_instance_class)
    error_message = "DB instance class must be a valid RDS instance type."
  }
}

variable "db_allocated_storage" {
  description = "Initial allocated storage for RDS instance (GB)"
  type        = number
  default     = 100
  
  validation {
    condition     = var.db_allocated_storage >= 20 && var.db_allocated_storage <= 65536
    error_message = "DB allocated storage must be between 20 and 65536 GB."
  }
}

variable "db_max_allocated_storage" {
  description = "Maximum allocated storage for RDS auto-scaling (GB)"
  type        = number
  default     = 500
  
  validation {
    condition     = var.db_max_allocated_storage >= var.db_allocated_storage
    error_message = "Maximum allocated storage must be greater than or equal to initial allocated storage."
  }
}

variable "db_backup_retention_days" {
  description = "Number of days to retain DB backups"
  type        = number
  default     = 30
  
  validation {
    condition     = var.db_backup_retention_days >= 0 && var.db_backup_retention_days <= 35
    error_message = "Backup retention days must be between 0 and 35."
  }
}

# ============================================================================
# REDIS CONFIGURATION
# ============================================================================

variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
  default     = "cache.r6g.large"
  
  validation {
    condition = contains([
      "cache.r6g.large", "cache.r6g.xlarge", "cache.r6g.2xlarge",
      "cache.r5.large", "cache.r5.xlarge", "cache.r5.2xlarge",
      "cache.t3.medium", "cache.t3.large"
    ], var.redis_node_type)
    error_message = "Redis node type must be a valid ElastiCache node type."
  }
}

variable "redis_num_cache_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 3
  
  validation {
    condition     = var.redis_num_cache_nodes >= 1 && var.redis_num_cache_nodes <= 6
    error_message = "Number of Redis cache nodes must be between 1 and 6."
  }
}

# ============================================================================
# ECS CONFIGURATION
# ============================================================================

variable "fastapi_image" {
  description = "Docker image for FastAPI application"
  type        = string
  default     = "ghcr.io/your-org/mlb-betting-system:latest"
}

variable "monitoring_image" {
  description = "Docker image for monitoring dashboard"
  type        = string
  default     = "ghcr.io/your-org/mlb-betting-monitoring:latest"
}

variable "fastapi_desired_count" {
  description = "Desired number of FastAPI tasks"
  type        = number
  default     = 3
  
  validation {
    condition     = var.fastapi_desired_count >= 1 && var.fastapi_desired_count <= 20
    error_message = "FastAPI desired count must be between 1 and 20."
  }
}

variable "fastapi_cpu" {
  description = "CPU units for FastAPI task (1024 = 1 vCPU)"
  type        = number
  default     = 1024
  
  validation {
    condition = contains([256, 512, 1024, 2048, 4096], var.fastapi_cpu)
    error_message = "FastAPI CPU must be one of: 256, 512, 1024, 2048, 4096."
  }
}

variable "fastapi_memory" {
  description = "Memory for FastAPI task (MB)"
  type        = number
  default     = 2048
  
  validation {
    condition     = var.fastapi_memory >= 512 && var.fastapi_memory <= 8192
    error_message = "FastAPI memory must be between 512 and 8192 MB."
  }
}

# ============================================================================
# AUTO SCALING CONFIGURATION
# ============================================================================

variable "autoscaling_min_capacity" {
  description = "Minimum number of tasks for auto scaling"
  type        = number
  default     = 2
  
  validation {
    condition     = var.autoscaling_min_capacity >= 1
    error_message = "Minimum capacity must be at least 1."
  }
}

variable "autoscaling_max_capacity" {
  description = "Maximum number of tasks for auto scaling"
  type        = number
  default     = 10
  
  validation {
    condition     = var.autoscaling_max_capacity >= var.autoscaling_min_capacity
    error_message = "Maximum capacity must be greater than or equal to minimum capacity."
  }
}

variable "autoscaling_target_cpu" {
  description = "Target CPU utilization percentage for auto scaling"
  type        = number
  default     = 70
  
  validation {
    condition     = var.autoscaling_target_cpu >= 20 && var.autoscaling_target_cpu <= 90
    error_message = "Target CPU utilization must be between 20 and 90 percent."
  }
}

variable "autoscaling_target_memory" {
  description = "Target memory utilization percentage for auto scaling"
  type        = number
  default     = 80
  
  validation {
    condition     = var.autoscaling_target_memory >= 20 && var.autoscaling_target_memory <= 90
    error_message = "Target memory utilization must be between 20 and 90 percent."
  }
}

# ============================================================================
# MONITORING CONFIGURATION
# ============================================================================

variable "enable_detailed_monitoring" {
  description = "Enable detailed CloudWatch monitoring"
  type        = bool
  default     = true
}

variable "enable_container_insights" {
  description = "Enable Container Insights for ECS"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days"
  type        = number
  default     = 30
  
  validation {
    condition = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch Logs retention period."
  }
}

variable "alarm_email_endpoints" {
  description = "Email addresses to receive alarm notifications"
  type        = list(string)
  default     = []
}

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

variable "enable_waf" {
  description = "Enable AWS WAF for the load balancer"
  type        = bool
  default     = true
}

variable "enable_shield" {
  description = "Enable AWS Shield Advanced"
  type        = bool
  default     = false
}

variable "ssl_policy" {
  description = "SSL policy for the load balancer"
  type        = string
  default     = "ELBSecurityPolicy-TLS-1-2-2017-01"
  
  validation {
    condition = contains([
      "ELBSecurityPolicy-TLS-1-2-2017-01",
      "ELBSecurityPolicy-TLS-1-2-Ext-2018-06",
      "ELBSecurityPolicy-FS-1-2-Res-2020-10"
    ], var.ssl_policy)
    error_message = "SSL policy must be a valid ELB security policy."
  }
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the application"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ============================================================================
# BACKUP CONFIGURATION
# ============================================================================

variable "enable_automated_backups" {
  description = "Enable automated backups"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 30
  
  validation {
    condition     = var.backup_retention_days >= 1 && var.backup_retention_days <= 365
    error_message = "Backup retention days must be between 1 and 365."
  }
}

variable "enable_cross_region_backup" {
  description = "Enable cross-region backup replication"
  type        = bool
  default     = true
}

variable "backup_destination_region" {
  description = "Destination region for cross-region backups"
  type        = string
  default     = "us-east-1"
}

# ============================================================================
# DISASTER RECOVERY CONFIGURATION
# ============================================================================

variable "enable_multi_az" {
  description = "Enable Multi-AZ deployment for RDS"
  type        = bool
  default     = true
}

variable "enable_read_replica" {
  description = "Enable read replica for RDS"
  type        = bool
  default     = false
}

variable "read_replica_regions" {
  description = "Regions for read replicas"
  type        = list(string)
  default     = []
}

# ============================================================================
# COST OPTIMIZATION CONFIGURATION
# ============================================================================

variable "enable_spot_instances" {
  description = "Enable Spot instances for ECS tasks (non-production only)"
  type        = bool
  default     = false
}

variable "reserved_instance_strategy" {
  description = "Reserved instance purchasing strategy"
  type        = string
  default     = "partial"
  
  validation {
    condition     = contains(["none", "partial", "full"], var.reserved_instance_strategy)
    error_message = "Reserved instance strategy must be one of: none, partial, full."
  }
}

variable "enable_cost_allocation_tags" {
  description = "Enable detailed cost allocation tags"
  type        = bool
  default     = true
}

# ============================================================================
# FEATURE FLAGS
# ============================================================================

variable "enable_api_caching" {
  description = "Enable API response caching"
  type        = bool
  default     = true
}

variable "enable_database_encryption" {
  description = "Enable database encryption at rest"
  type        = bool
  default     = true
}

variable "enable_transit_encryption" {
  description = "Enable encryption in transit"
  type        = bool
  default     = true
}

variable "enable_access_logs" {
  description = "Enable access logs for load balancer"
  type        = bool
  default     = true
}

variable "enable_vpc_flow_logs" {
  description = "Enable VPC Flow Logs"
  type        = bool
  default     = true
}

# ============================================================================
# EXPERIMENTAL FEATURES
# ============================================================================

variable "enable_fargate_spot" {
  description = "Enable Fargate Spot for cost optimization (experimental)"
  type        = bool
  default     = false
}

variable "enable_predictive_scaling" {
  description = "Enable predictive auto scaling (experimental)"
  type        = bool
  default     = false
}

variable "enable_chaos_engineering" {
  description = "Enable chaos engineering tools (experimental)"
  type        = bool
  default     = false
}

# ============================================================================
# CUSTOM TAGS
# ============================================================================

variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}