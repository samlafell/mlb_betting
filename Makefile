# MLB ML Prediction System - Development Makefile
# Simplifies common Docker Compose operations

.PHONY: help setup start stop restart logs health test clean

# Default target
help:
	@echo "MLB ML Prediction System - Available Commands:"
	@echo ""
	@echo "Setup & Management:"
	@echo "  make setup     - Initial setup (copy .env, setup database)"
	@echo "  make start     - Start all services"
	@echo "  make stop      - Stop all services" 
	@echo "  make restart   - Restart all services"
	@echo "  make clean     - Stop and remove all containers/volumes"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs      - Show all service logs"
	@echo "  make health    - Check API health"
	@echo "  make status    - Show container status"
	@echo "  make test      - Run basic API tests
  make test-full - Run comprehensive integration tests
  make recover   - Intelligent container recovery"
	@echo ""
	@echo "Development:"
	@echo "  make build     - Build/rebuild containers"
	@echo "  make shell     - Open shell in FastAPI container"
	@echo "  make db-setup  - Setup ML database tables"
	@echo ""
	@echo "Individual Services:"
	@echo "  make logs-api    - FastAPI logs"
	@echo "  make logs-redis  - Redis logs"
	@echo "  make logs-mlflow - MLflow logs"
	@echo "  make logs-nginx  - Nginx logs"

# Setup and initialization
setup:
	@echo "🚀 Setting up MLB ML Prediction System..."
	@if [ ! -f .env ]; then \
		echo "📝 Copying .env template..."; \
		cp .env.example .env; \
		echo "⚠️  Please edit .env with your database credentials!"; \
	else \
		echo "✅ .env file already exists"; \
	fi
	@echo "🗄️  Setting up ML database tables..."
	@$(MAKE) db-setup
	@echo "✅ Setup complete! Run 'make start' to launch services."

# Database setup
db-setup:
	@echo "📊 Setting up ML database tables..."
	@uv run docker/scripts/simple_ml_database_setup.py

# Service management
start:
	@echo "🚀 Starting MLB ML services..."
	@docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 15
	@$(MAKE) health

stop:
	@echo "🛑 Stopping MLB ML services..."
	@docker-compose down

restart:
	@echo "🔄 Restarting MLB ML services..."
	@docker-compose restart
	@sleep 10
	@$(MAKE) health

# Monitoring and testing
health:
	@echo "🏥 Checking API health..."
	@curl -s http://localhost/health | jq '.' || echo "❌ Health check failed"

status:
	@echo "📊 Container status:"
	@docker-compose ps

test:
	@echo "🧪 Running basic API tests..."
	@echo "Testing health endpoint..."
	@curl -s http://localhost/health > /dev/null && echo "✅ Health OK" || echo "❌ Health failed"
	@echo "Testing root endpoint..."
	@curl -s http://localhost/ > /dev/null && echo "✅ Root OK" || echo "❌ Root failed"
	@echo "Testing prediction endpoint..."
	@curl -s http://localhost/api/v1/predict/test_game > /dev/null && echo "✅ Predictions OK" || echo "❌ Predictions failed"
	@echo "Testing models endpoint..."
	@curl -s http://localhost/api/v1/models/active > /dev/null && echo "✅ Models OK" || echo "❌ Models failed"

# Comprehensive integration tests
test-full:
	@echo "🧪 Running comprehensive integration tests..."
	@./scripts/test-docker-stack.sh

# Intelligent container recovery
recover:
	@echo "🔧 Starting intelligent container recovery..."
	@./scripts/docker-recovery.sh recover

# Logging
logs:
	@docker-compose logs -f

logs-api:
	@docker-compose logs -f fastapi

logs-redis:
	@docker-compose logs -f redis

logs-mlflow:
	@docker-compose logs -f mlflow

logs-nginx:
	@docker-compose logs -f nginx

# Development
build:
	@echo "🔨 Building/rebuilding containers..."
	@docker-compose build

shell:
	@echo "🐚 Opening shell in FastAPI container..."
	@docker-compose exec fastapi /bin/bash

# Cleanup
clean:
	@echo "🧹 Cleaning up all containers and volumes..."
	@docker-compose down -v
	@docker system prune -f
	@echo "✅ Cleanup complete"

# Quick development workflow
dev: setup start
	@echo "🎉 Development environment ready!"
	@echo "📡 API available at: http://localhost"
	@echo "📊 MLflow UI at: http://localhost:5001"
	@echo "📖 Run 'make logs' to see service logs"
	@echo "🏥 Run 'make health' to check service status"

# Production-like testing
prod-test: build start
	@sleep 30
	@$(MAKE) test
	@$(MAKE) logs

# Memory usage check
memory:
	@echo "💾 Checking container memory usage..."
	@docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"