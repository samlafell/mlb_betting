#!/bin/bash
# 🔐 Secure Password Generator for MLB Betting System
# Run this script to generate secure passwords for your .env file

set -e

echo "🔐 MLB Betting System - Secure Password Generator"
echo "=================================================="
echo ""

# Function to generate a secure password
generate_password() {
    openssl rand -base64 32 | tr -d "=+/" | cut -c1-25
}

echo "📋 Generated Secure Passwords:"
echo "==============================="
echo ""

echo "# PostgreSQL Database"
echo "POSTGRES_PASSWORD=$(generate_password)"
echo ""

echo "# Redis (optional)"
echo "REDIS_PASSWORD=$(generate_password)"
echo ""

echo "# MLflow (optional)"
echo "MLFLOW_TRACKING_PASSWORD=$(generate_password)"
echo ""

echo "# API Security"
echo "API_SECRET_KEY=$(generate_password)"
echo "JWT_SECRET_KEY=$(generate_password)"
echo ""

echo "🚨 CRITICAL SECURITY INSTRUCTIONS:"
echo "=================================="
echo "1. Copy the passwords above to your .env file"
echo "2. NEVER commit .env files to version control"
echo "3. Store passwords securely (password manager recommended)"
echo "4. Rotate passwords regularly in production"
echo "5. Use different passwords for each environment (dev/staging/prod)"
echo ""

echo "✅ Copy .env.example to .env and replace the placeholder passwords"
echo "   cp .env.example .env"
echo ""

echo "🔒 For additional security, see SECURITY.md"