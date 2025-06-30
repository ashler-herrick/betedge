#!/bin/bash

# ThetaData Environment Configuration Switcher
# Usage: ./set_env.sh --dev | ./set_env.sh --prod | ./set_env.sh --stage

CONFIG_FILE="/home/ashler/ThetaData/ThetaTerminal/config_0.properties"
BACKUP_FILE="/home/ashler/ThetaData/ThetaTerminal/config_0.properties.backup"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "ThetaData Environment Configuration Switcher"
    echo ""
    echo "Usage: $0 [--dev|--prod|--stage] [options]"
    echo ""
    echo "Environments:"
    echo "  --dev     Set to development environment (FPSS_DEV_HOSTS)"
    echo "            - Frequent reboots, constant historical data replay"
    echo "            - Use for development outside market hours"
    echo ""
    echo "  --prod    Set to production environment (FPSS_NJ_HOSTS)" 
    echo "            - Stable production servers in New Jersey"
    echo "            - Use for live trading and production"
    echo ""
    echo "  --stage   Set to staging environment (FPSS_STAGE_HOSTS)"
    echo "            - Occasional reboots, testing environment"
    echo "            - Use for pre-production testing"
    echo ""
    echo "Options:"
    echo "  --backup  Create backup before making changes (default: true)"
    echo "  --status  Show current configuration"
    echo "  --help    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --dev              # Switch to development environment"
    echo "  $0 --prod             # Switch to production environment"  
    echo "  $0 --status           # Show current configuration"
    echo ""
}

# Function to check if config file exists
check_config_file() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        print_error "Config file not found: $CONFIG_FILE"
        print_info "Make sure ThetaTerminal is installed and has been run at least once"
        exit 1
    fi
}

# Function to create backup
create_backup() {
    if [[ "$1" == "true" ]]; then
        print_info "Creating backup of current configuration..."
        cp "$CONFIG_FILE" "$BACKUP_FILE"
        if [[ $? -eq 0 ]]; then
            print_success "Backup created: $BACKUP_FILE"
        else
            print_error "Failed to create backup"
            exit 1
        fi
    fi
}

# Function to get current configuration
get_current_config() {
    local fpss_region=$(grep "^FPSS_REGION=" "$CONFIG_FILE" | cut -d'=' -f2)
    local mdds_region=$(grep "^MDDS_REGION=" "$CONFIG_FILE" | cut -d'=' -f2)
    
    echo "Current Configuration:"
    echo "  FPSS_REGION: $fpss_region"
    echo "  MDDS_REGION: $mdds_region"
    echo ""
    
    case "$fpss_region" in
        "FPSS_NJ_HOSTS")
            echo "  Environment: PRODUCTION"
            echo "  Description: Stable production servers"
            ;;
        "FPSS_DEV_HOSTS")
            echo "  Environment: DEVELOPMENT" 
            echo "  Description: Development servers with historical data replay"
            ;;
        "FPSS_STAGE_HOSTS")
            echo "  Environment: STAGING"
            echo "  Description: Testing servers with occasional reboots"
            ;;
        *)
            echo "  Environment: PRODUCTION (default)"
            echo "  Description: Using FPSS_NJ_HOSTS (production servers)"
            ;;
    esac
}

# Function to update configuration
update_config() {
    local fpss_region="$1"
    local mdds_region="$2"
    local env_name="$3"
    
    print_info "Updating configuration to $env_name environment..."
    
    # Use sed to update the configuration file
    sed -i "s/^FPSS_REGION=.*/FPSS_REGION=$fpss_region/" "$CONFIG_FILE"
    sed -i "s/^MDDS_REGION=.*/MDDS_REGION=$mdds_region/" "$CONFIG_FILE"
    
    if [[ $? -eq 0 ]]; then
        print_success "Configuration updated successfully!"
        echo ""
        print_info "New configuration:"
        echo "  FPSS_REGION: $fpss_region"
        echo "  MDDS_REGION: $mdds_region"
        echo ""
        print_warning "Please restart ThetaTerminal for changes to take effect"
    else
        print_error "Failed to update configuration"
        exit 1
    fi
}

# Function to verify terminal is not running
check_terminal_running() {
    if pgrep -f "ThetaTerminal.jar" > /dev/null; then
        print_warning "ThetaTerminal appears to be running"
        print_info "For best results, stop the terminal before changing configuration"
        echo ""
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Aborted by user"
            exit 0
        fi
    fi
}

# Parse command line arguments
ENVIRONMENT=""
CREATE_BACKUP="true"
SHOW_STATUS="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            ENVIRONMENT="dev"
            shift
            ;;
        --prod)
            ENVIRONMENT="prod"
            shift
            ;;
        --stage)
            ENVIRONMENT="stage"
            shift
            ;;
        --backup)
            CREATE_BACKUP="true"
            shift
            ;;
        --no-backup)
            CREATE_BACKUP="false"
            shift
            ;;
        --status)
            SHOW_STATUS="true"
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
done

# Main logic
check_config_file

if [[ "$SHOW_STATUS" == "true" ]]; then
    get_current_config
    exit 0
fi

if [[ -z "$ENVIRONMENT" ]]; then
    print_error "No environment specified"
    echo ""
    show_usage
    exit 1
fi

# Show current configuration
echo "ThetaData Environment Switcher"
echo "=============================="
echo ""
get_current_config
echo ""

# Check if terminal is running
check_terminal_running

# Create backup if requested
create_backup "$CREATE_BACKUP"

# Update configuration based on environment
case "$ENVIRONMENT" in
    "dev")
        update_config "FPSS_DEV_HOSTS" "MDDS_DEV_HOSTS" "DEVELOPMENT"
        echo ""
        print_info "Development environment features:"
        print_info "• Historical data replay outside market hours"
        print_info "• Frequent server reboots (expected)"
        print_info "• Perfect for development and testing"
        ;;
    "prod")
        update_config "FPSS_NJ_HOSTS" "MDDS_NJ_HOSTS" "PRODUCTION"
        echo ""
        print_info "Production environment features:"
        print_info "• Stable production servers"
        print_info "• Live market data during market hours"
        print_info "• Use for production trading systems"
        ;;
    "stage")
        update_config "FPSS_STAGE_HOSTS" "MDDS_STAGE_HOSTS" "STAGING"
        echo ""
        print_info "Staging environment features:"
        print_info "• Testing environment with occasional reboots"
        print_info "• Use for pre-production testing"
        print_info "• More stable than dev, less stable than prod"
        ;;
esac

echo ""
print_success "Environment switch completed!"
print_warning "Remember to restart ThetaTerminal to apply changes"