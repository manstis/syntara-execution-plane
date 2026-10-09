#!/usr/bin/env bash
# Generate secrets for Execution Plane local development.
#
# This script generates:
# - A base64url-encoded 32-byte AES-256 credential encryption key
# - An ES256 public key used to verify AO service tokens
#
# If a sibling Syntara checkout already has jwt-primary.pub, that key is copied
# so this stack can verify tokens issued by local AO. Otherwise a standalone
# key pair is generated under .secrets/.
#
# Usage:
#   ./tools/generate_secrets.sh         # Generate secrets if they don't exist
#   ./tools/generate_secrets.sh --force # Regenerate all secrets

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SECRETS_DIR="$PROJECT_ROOT/.secrets"
SYNTARA_ROOT="${SYNTARA_ROOT:-$PROJECT_ROOT/../syntara}"
SYNTARA_JWT_PUB="$SYNTARA_ROOT/backend/.secrets/jwt-primary.pub"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

clear_path() {
    # Podman creates empty directories when bind-mounting a missing host path;
    # those stubs block later writes.
    local path="$1"
    if [[ -L "$path" || -f "$path" ]]; then
        rm -f "$path"
    elif [[ -d "$path" ]]; then
        warn "Removing directory stub created by a bind mount of a missing file: $path"
        rm -rf "$path"
    fi
}

check_dependencies() {
    if ! command -v openssl &> /dev/null; then
        error "openssl is required but not installed."
        exit 1
    fi
}

generate_encryption_key() {
    # 32 raw bytes, encoded as URL-safe base64 with padding (matches EPSettings).
    openssl rand 32 | openssl base64 -A | tr '+/' '-_'
}

generate_key_pair() {
    local key_name="$1"
    local private_key_path="$SECRETS_DIR/${key_name}.pem"
    local public_key_path="$SECRETS_DIR/${key_name}.pub"

    info "Generating $key_name key pair..."
    clear_path "$private_key_path"
    clear_path "$public_key_path"
    openssl ecparam -name prime256v1 -genkey -noout -out "$private_key_path"
    openssl ec -in "$private_key_path" -pubout -out "$public_key_path" 2>/dev/null
    chmod 600 "$private_key_path"
    chmod 644 "$public_key_path"
    info "  Private key: $private_key_path"
    info "  Public key:  $public_key_path"
}

main() {
    local force=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --force|-f)
                force=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [--force]"
                echo ""
                echo "Generate encryption, EP command signing and AO assertion verification keys."
                echo ""
                echo "Options:"
                echo "  --force, -f  Regenerate all keys even if they exist"
                echo "  --help, -h   Show this help message"
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                exit 1
                ;;
        esac
    done

    check_dependencies

    if [[ ! -d "$SECRETS_DIR" ]]; then
        info "Creating secrets directory: $SECRETS_DIR"
    fi
    mkdir -p "$SECRETS_DIR"
    chmod 700 "$SECRETS_DIR"

    if [[ -f "$SECRETS_DIR/encryption-key" ]] && [[ "$force" != true ]]; then
        info "Encryption key already exists, skipping (use --force to regenerate)"
    else
        info "Generating credential encryption key..."
        clear_path "$SECRETS_DIR/encryption-key"
        generate_encryption_key > "$SECRETS_DIR/encryption-key"
        info "  Encryption key: $SECRETS_DIR/encryption-key"
    fi

    local jwt_pub="$SECRETS_DIR/ao-jwt-public.pem"
    if [[ -f "$jwt_pub" ]] && [[ "$force" != true ]]; then
        info "AO JWT public key already exists, skipping (use --force to regenerate)"
    elif [[ -f "$SYNTARA_JWT_PUB" ]]; then
        info "Copying AO JWT public key from sibling Syntara checkout"
        clear_path "$jwt_pub"
        cp "$SYNTARA_JWT_PUB" "$jwt_pub"
        chmod 644 "$jwt_pub"
        info "  Public key: $jwt_pub"
    else
        warn "Sibling Syntara JWT public key not found at $SYNTARA_JWT_PUB"
        warn "Generating a standalone ES256 key pair for local API startup"
        generate_key_pair "jwt-primary"
        clear_path "$jwt_pub"
        cp "$SECRETS_DIR/jwt-primary.pub" "$jwt_pub"
        chmod 644 "$jwt_pub"
        info "  Public key: $jwt_pub"
        info "  Point EP_AO_JWT_PUBLIC_KEY_FILE at AO's jwt-primary.pub to verify real Syntara tokens"
    fi

    if [[ ! -f "$SECRETS_DIR/ep-command.pem" || "$force" == true ]]; then
        generate_key_pair "ep-command"
    fi
    # Keep the host secret directory private; the container's UID must read this bind mount.
    chmod 644 "$SECRETS_DIR/ep-command.pem"
    if [[ ! -f "$SECRETS_DIR/command-peers.json" ]]; then
        cp "$PROJECT_ROOT/deploy/command-peers.example.json" "$SECRETS_DIR/command-peers.json"
        info "Review peer URLs, certificate names and network ranges in command-peers.json before startup"
    fi

    # Bind-mounted files must be readable by container UID 1001. Keep private
    # keys 0600; the 0700 directory is what blocks other local users.
    if [[ -f "$SECRETS_DIR/encryption-key" ]]; then
        chmod 644 "$SECRETS_DIR/encryption-key"
    fi
    if [[ -f "$jwt_pub" ]]; then
        chmod 644 "$jwt_pub"
    fi
    if [[ -f "$SECRETS_DIR/jwt-primary.pem" ]]; then
        chmod 600 "$SECRETS_DIR/jwt-primary.pem"
    fi
    if [[ -f "$SECRETS_DIR/jwt-primary.pub" ]]; then
        chmod 644 "$SECRETS_DIR/jwt-primary.pub"
    fi

    echo ""
    info "Secrets are ready in $SECRETS_DIR"
    info "  EP_CREDENTIAL_ENCRYPTION_KEY_PATH=/run/secrets/ep/credential-encryption-key"
    info "  EP_AO_JWT_PUBLIC_KEY_PATH=/run/secrets/ao-jwt-public.pem"
}

main "$@"
