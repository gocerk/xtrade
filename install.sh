#!/bin/bash

# Exit on error
set -e

echo "Starting XTRADE dependency installation for Ubuntu 20.04..."

# 1. Update System
echo "Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

# 2. Install Basic Tools
echo "Installing curl, git, and build-essential..."
sudo apt-get install -y curl git build-essential

# 3. Install NVM and Node.js (Latest)
echo "Installing NVM and latest Node.js..."
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

# Load NVM environment variables
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

# Install latest Node.js
nvm install node
nvm use node
nvm alias default node

# Verify Node.js installation
node_version=$(node -v)
npm_version=$(npm -v)
echo "Node.js version: $node_version"
echo "NPM version: $npm_version"

# 4. Install MySQL Server
echo "Installing MySQL Server..."
sudo apt-get install -y mysql-server

# Start and Enable MySQL
sudo systemctl start mysql
sudo systemctl enable mysql

# 5. Configure Database
echo "Configuring MySQL Database..."
DB_NAME="TraderProBOT"
DB_USER="root"
# Note: In a production script, avoid hardcoding passwords or using empty ones if possible.
# For this script, we'll use the current system user or root with socket authentication default in Ubuntu 20.04.

echo "Creating database '$DB_NAME' if it doesn't exist..."
sudo mysql -e "CREATE DATABASE IF NOT EXISTS $DB_NAME;"

# Check for SQL dump files to import
# Prioritize xx.sql as requested, otherwise fall back to axsd.sql
if [ -f "xx.sql" ]; then
    SQL_FILE="xx.sql"
elif [ -f "axsd.sql" ]; then
    SQL_FILE="axsd.sql"
else
    SQL_FILE=""
fi

if [ -n "$SQL_FILE" ]; then
    echo "Found $SQL_FILE. Importing into $DB_NAME..."
    sudo mysql $DB_NAME < $SQL_FILE
    echo "Import complete."
else
    echo "Warning: No SQL dump file (xx.sql or axsd.sql) found. Please import your database schema manually."
    echo "Available .sql files:"
    ls *.sql 2>/dev/null || echo "No .sql files found."
fi

# 6. Install Project Dependencies
echo "Installing project dependencies via NPM..."
if [ -f "package.json" ]; then
    npm install
else
    echo "Error: package.json not found in the current directory."
    exit 1
fi

# 7. Install PM2 Process Manager
echo "Installing PM2 globally..."
sudo npm install -g pm2

# 8. Final Instructions
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo "1. Check your .env file for database configuration."
echo "   (Default in db.js is DB_USER=root, DB_PASSWORD='', DB_NAME=TraderProBOT)"
echo "2. To start the application using PM2:"
echo "   pm2 start ecosystem.config.js"
echo "   OR"
echo "   pm2 start server.js --name xtrade"
echo "3. To save PM2 list on reboot:"
echo "   pm2 save"
echo "   pm2 startup"
echo "=========================================="

