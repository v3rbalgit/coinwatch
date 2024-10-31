# Makefile targets are phony (no files to check)
.PHONY: help test-build test-up test-down test-logs prod-build prod-up prod-down prod-logs clean build-all stop-all

# Use bash for shell commands
SHELL := /bin/bash

# Colors
RED=$(shell tput setaf 1)
GREEN=$(shell tput setaf 2)
YELLOW=$(shell tput setaf 3)
BLUE=$(shell tput setaf 4)
PURPLE=$(shell tput setaf 5)
CYAN=$(shell tput setaf 6)
WHITE=$(shell tput setaf 7)
RESET=$(shell tput sgr0)

help: ## Show this help message
	@echo "${BLUE}Usage:${RESET}"
	@echo "  make ${GREEN}<target>${RESET}"
	@echo
	@echo "${BLUE}Targets:${RESET}"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  ${GREEN}%-15s${RESET} %s\n", $$1, $$2}'
	@echo
	@echo "${YELLOW}Examples:${RESET}"
	@echo "  make test-up       # Start test environment"
	@echo "  make prod-up       # Start production environment"
	@echo "  make clean         # Clean all containers and volumes"

# Testing Environment
test-build: ## Build test environment containers
	docker-compose -f docker-compose.test.yml build

test-up: ## Start test environment
	docker-compose -f docker-compose.test.yml up -d db
	@echo "Waiting for database to be ready..."
	@sleep 30
	docker-compose -f docker-compose.test.yml up app

test-down: ## Stop test environment
	docker-compose -f docker-compose.test.yml down

test-logs: ## View test environment logs
	docker-compose -f docker-compose.test.yml logs -f

test-db: ## Access test database through phpMyAdmin
	docker-compose -f docker-compose.test.yml up -d phpmyadmin
	@echo "PhpMyAdmin available at http://localhost:8080"
	@echo "Server: db"
	@echo "Username: test_user"
	@echo "Password: test_password"

test-shell: ## Open a shell in the test app container
	docker-compose -f docker-compose.test.yml exec app bash

# Production Environment
prod-build: ## Build production environment containers
	docker-compose -f docker-compose.prod.yml build

prod-up: ## Start production environment
	docker-compose -f docker-compose.prod.yml up -d db
	@echo "Waiting for database to be ready..."
	@sleep 30
	docker-compose -f docker-compose.prod.yml up -d app

prod-down: ## Stop production environment
	docker-compose -f docker-compose.prod.yml down

prod-logs: ## View production logs
	docker-compose -f docker-compose.prod.yml logs -f

prod-status: ## Check status of production services
	docker-compose -f docker-compose.prod.yml ps

prod-shell: ## Open a shell in the production app container
	docker-compose -f docker-compose.prod.yml exec app bash

# Database Management
db-backup: ## Backup production database
	@mkdir -p backups
	docker-compose -f docker-compose.prod.yml exec db mysqldump -u prod_user -pprod_password coinwatch > backups/backup-$$(date +%Y%m%d-%H%M%S).sql

db-restore: ## Restore latest database backup
	@latest=$$(ls -t backups/*.sql | head -1); \
	if [ -n "$$latest" ]; then \
		docker-compose -f docker-compose.prod.yml exec -T db mysql -u prod_user -pprod_password coinwatch < $$latest; \
		echo "Restored $$latest"; \
	else \
		echo "No backup found"; \
	fi

# Utility Commands
clean: ## Remove all containers, volumes, and images
	docker-compose -f docker-compose.test.yml down -v
	docker-compose -f docker-compose.prod.yml down -v
	docker system prune -f

build-all: ## Build both test and production environments
	make test-build
	make prod-build

stop-all: ## Stop all environments
	make test-down
	make prod-down

logs: ## View all logs (both test and prod)
	@tmux new-session \; \
		split-window -h \; \
		send-keys 'make test-logs' C-m \; \
		select-pane -t 0 \; \
		send-keys 'make prod-logs' C-m

# Development Utilities
lint: ## Run linter
	docker-compose -f docker-compose.test.yml run --rm app pylint src/

test: ## Run tests
	docker-compose -f docker-compose.test.yml run --rm app pytest

coverage: ## Run tests with coverage report
	docker-compose -f docker-compose.test.yml run --rm app pytest --cov=src --cov-report=term-missing