"""
One-time setup script: creates the Databricks secret scope and stores the
Massive API key. Run this locally (with the Databricks CLI configured) or
from a notebook - never commit the resulting secret value anywhere.

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.errors import ResourceAlreadyExists
import getpass

w = WorkspaceClient()

# Helper function to create scope if it doesn't exist
def create_scope_if_not_exists(scope_name):
    try:
        w.secrets.create_scope(scope=scope_name)
        print(f"✓ Created secret scope: {scope_name}")
    except ResourceAlreadyExists:
        print(f"ℹ Secret scope '{scope_name}' already exists, skipping creation")

# Create scopes (will skip if they already exist)
create_scope_if_not_exists("massive")
create_scope_if_not_exists("database")
create_scope_if_not_exists("finnhub")

# Add/update secrets
print("\nSetting up secrets...")
w.secrets.put_secret(
    scope="massive",
    key="api-key",
    string_value=getpass.getpass("Paste your Massive API key: ")
)
print("✓ Stored Massive API key")

w.secrets.put_secret(
    scope="database",
    key="lakebase-url",
    string_value=getpass.getpass("Paste your Lakebase URL: ")
)
print("✓ Stored Lakebase URL")

w.secrets.put_secret(
    scope="finnhub",
    key="api-key",
    string_value=getpass.getpass("Paste your Finnhub API key: ")
)
print("✓ Stored Finnhub API key")

# Set ACLs for all scopes
print("\nSetting permissions...")
for scope_name in ["database", "massive", "finnhub"]:
    try:
        w.secrets.put_acl(
            scope=scope_name,
            principal="users",
            permission=workspace.AclPermission.READ,
        )
        print(f"✓ Set READ permission for '{scope_name}' scope")
    except Exception as e:
        print(f"⚠ Warning: Could not set ACL for '{scope_name}': {e}")

print("\n🎉 Setup complete! All secrets are configured.")
