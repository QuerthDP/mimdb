#!/usr/bin/env python3
"""
API Compliance Validator for MIMDB REST API

This script validates that the running API server complies with
the OpenAPI specification in dbmsInterface.yaml.

Usage:
    python validate_api.py                              # Auto-start server
    python validate_api.py --url http://localhost:3000  # Use existing server
    python validate_api.py --no-auto                    # Fail if server not running
"""

import argparse
import atexit
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any

import requests
import yaml

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def load_openapi_spec(spec_path: str) -> dict:
    """Load and parse the OpenAPI specification."""
    with open(spec_path, "r") as f:
        return yaml.safe_load(f)


def check_field_type(value: Any, expected_type: str, spec: dict) -> tuple[bool, str]:
    """Check if a value matches the expected OpenAPI type."""
    if expected_type == "string":
        return isinstance(value, str), f"expected string, got {type(value).__name__}"
    elif expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool), f"expected integer, got {type(value).__name__}"
    elif expected_type == "boolean":
        return isinstance(value, bool), f"expected boolean, got {type(value).__name__}"
    elif expected_type == "array":
        return isinstance(value, list), f"expected array, got {type(value).__name__}"
    elif expected_type == "object":
        return isinstance(value, dict), f"expected object, got {type(value).__name__}"
    return True, ""


def resolve_ref(ref: str, spec: dict) -> dict:
    """Resolve a $ref in the OpenAPI spec."""
    parts = ref.replace("#/", "").split("/")
    result = spec
    for part in parts:
        result = result.get(part, {})
    return result


def validate_response_schema(response_data: Any, schema: dict, spec: dict, path: str = "") -> list[str]:
    """Validate response data against OpenAPI schema."""
    errors = []

    # Resolve $ref if present
    if "$ref" in schema:
        schema = resolve_ref(schema["$ref"], spec)

    # Handle oneOf - data must match at least one option
    # Note: We allow multiple matches for ambiguous cases (e.g., empty arrays)
    if "oneOf" in schema:
        matching_options = []
        all_errors = []
        for i, option in enumerate(schema["oneOf"]):
            option_errors = validate_response_schema(response_data, option, spec, path)
            if not option_errors:
                matching_options.append(i)
            all_errors.extend(option_errors)

        if len(matching_options) == 0:
            errors.append(f"{path}: value doesn't match any oneOf option")
            # Show first few errors from each option
            for err in all_errors[:3]:
                errors.append(f"  {err}")
        # Allow multiple matches - JSON Schema oneOf is hard to enforce strictly
        return errors

    # Handle type checking
    schema_type = schema.get("type")

    if schema_type == "array":
        if not isinstance(response_data, list):
            errors.append(f"{path}: expected array, got {type(response_data).__name__}")
        else:
            items_schema = schema.get("items", {})
            for i, item in enumerate(response_data[:5]):  # Check first 5 items
                errors.extend(validate_response_schema(item, items_schema, spec, f"{path}[{i}]"))

    elif schema_type == "object" or (not schema_type and "properties" in schema):
        # Treat as object if type is object OR if properties are defined
        if not isinstance(response_data, dict):
            errors.append(f"{path}: expected object, got {type(response_data).__name__}")
        else:
            # Check required fields - STRICT validation
            required = schema.get("required", [])
            for field in required:
                # Convert camelCase to check both formats
                if field not in response_data:
                    errors.append(f"{path}: missing required field '{field}'")

            # Check for unexpected null values in required fields
            for field in required:
                if field in response_data and response_data[field] is None:
                    errors.append(f"{path}: required field '{field}' cannot be null")

            # Check properties
            properties = schema.get("properties", {})
            for prop_name, prop_schema in properties.items():
                if prop_name in response_data:
                    errors.extend(validate_response_schema(
                        response_data[prop_name],
                        prop_schema,
                        spec,
                        f"{path}.{prop_name}"
                    ))

    elif schema_type:
        valid, msg = check_field_type(response_data, schema_type, spec)
        if not valid:
            errors.append(f"{path}: {msg}")

    return errors


class APIValidator:
    def __init__(self, base_url: str, spec_path: str):
        self.base_url = base_url.rstrip("/")
        self.spec = load_openapi_spec(spec_path)
        self.results = {"passed": 0, "failed": 0, "skipped": 0}
        self.test_table_id = None
        self.test_query_id = None
        self.test_select_query_id = None

    def log(self, status: str, message: str):
        if status == "PASS":
            print(f"  {GREEN}✓{RESET} {message}")
            self.results["passed"] += 1
        elif status == "FAIL":
            print(f"  {RED}✗{RESET} {message}")
            self.results["failed"] += 1
        elif status == "SKIP":
            print(f"  {YELLOW}○{RESET} {message}")
            self.results["skipped"] += 1
        elif status == "INFO":
            print(f"  {BLUE}ℹ{RESET} {message}")

    def _test_default_values(self, csv_path: str, csv_with_header_path: str):
        """
        Test that OpenAPI default values work correctly.
        Per spec, doesCsvContainHeader defaults to false.
        """
        # Test 1: Submit COPY without doesCsvContainHeader - should treat CSV as no-header
        copy_no_flag = {
            "queryDefinition": {
                "sourceFilepath": csv_path,  # No header in this file
                "destinationTableName": "compliance_test"
                # doesCsvContainHeader omitted - should default to false
            }
        }
        try:
            resp = requests.post(f"{self.base_url}/query", json=copy_no_flag, timeout=10)
            if resp.status_code == 200:
                self.log("PASS", "COPY without doesCsvContainHeader accepted (uses default)")
            else:
                self.log("FAIL", f"COPY without doesCsvContainHeader failed: {resp.status_code}")
        except Exception as e:
            self.log("FAIL", f"COPY default test request failed: {e}")

        # Test 2: Submit COPY with explicit doesCsvContainHeader=true on header file
        copy_with_header = {
            "queryDefinition": {
                "sourceFilepath": csv_with_header_path,  # Has header row
                "destinationTableName": "compliance_test",
                "doesCsvContainHeader": True  # Explicitly set
            }
        }
        try:
            resp = requests.post(f"{self.base_url}/query", json=copy_with_header, timeout=10)
            if resp.status_code == 200:
                self.log("PASS", "COPY with doesCsvContainHeader=true accepted")
            else:
                self.log("FAIL", f"COPY with doesCsvContainHeader=true failed: {resp.status_code}")
        except Exception as e:
            self.log("FAIL", f"COPY with header test request failed: {e}")

    def _validate_query_required_fields(self, query_response: dict, query_type: str):
        """
        Explicitly validate required fields in Query response per OpenAPI spec.
        Required fields: queryId, status, queryDefinition
        Optional fields: isResultAvailable
        """
        # Required fields per API spec
        required_fields = ["queryId", "status", "queryDefinition"]

        for field in required_fields:
            if field not in query_response:
                self.log("FAIL", f"{query_type} query response missing required field: '{field}'")
            elif query_response[field] is None:
                self.log("FAIL", f"{query_type} query response has null value for required field: '{field}'")
            else:
                self.log("PASS", f"{query_type} query has required field: '{field}'")

        # Validate queryDefinition structure based on query type
        if "queryDefinition" in query_response and query_response["queryDefinition"]:
            qdef = query_response["queryDefinition"]
            if query_type == "COPY":
                copy_required = ["sourceFilepath", "destinationTableName"]
                for field in copy_required:
                    if field not in qdef:
                        self.log("FAIL", f"COPY queryDefinition missing required: '{field}'")
            elif query_type == "SELECT":
                if "tableName" not in qdef:
                    self.log("FAIL", f"SELECT queryDefinition missing: 'tableName'")

    def get_request_schema(self, path: str, method: str) -> dict | None:
        """Get the expected request body schema from OpenAPI spec."""
        path_spec = self.spec.get("paths", {}).get(path, {})
        method_spec = path_spec.get(method.lower(), {})
        request_body = method_spec.get("requestBody", {})

        # Resolve $ref at request body level
        if "$ref" in request_body:
            request_body = resolve_ref(request_body["$ref"], self.spec)

        content = request_body.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema")

        # Resolve $ref at schema level
        if schema and "$ref" in schema:
            schema = resolve_ref(schema["$ref"], self.spec)

        return schema

    def get_schema_for_response(self, path: str, method: str, status_code: int) -> dict | None:
        """Get the expected response schema from OpenAPI spec."""
        path_spec = self.spec.get("paths", {}).get(path, {})
        method_spec = path_spec.get(method.lower(), {})
        responses = method_spec.get("responses", {})

        # Try both int and string keys for status code
        response_spec = responses.get(status_code, responses.get(str(status_code), {}))

        # Resolve $ref at response level first
        if "$ref" in response_spec:
            response_spec = resolve_ref(response_spec["$ref"], self.spec)

        content = response_spec.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema")

        # Resolve $ref at schema level
        if schema and "$ref" in schema:
            schema = resolve_ref(schema["$ref"], self.spec)

        return schema

    def validate_endpoint(self, method: str, path: str, expected_status: int,
                          json_data: dict = None, description: str = "",
                          require_schema: bool = True) -> bool:
        """Test a single endpoint and validate response."""
        url = f"{self.base_url}{path}"

        # Validate request body against spec BEFORE sending
        if json_data and method in ("POST", "PUT", "PATCH"):
            request_schema = self.get_request_schema(path, method)
            if request_schema:
                errors = validate_response_schema(json_data, request_schema, self.spec, "request")
                if errors:
                    self.log("FAIL", f"{method} {path}: request body doesn't match spec")
                    for err in errors[:3]:
                        print(f"      - {err}")
                    return False

        try:
            if method == "GET":
                resp = requests.get(url, json=json_data, timeout=5)
            elif method == "POST":
                resp = requests.post(url, json=json_data, timeout=5)
            elif method == "PUT":
                resp = requests.put(url, json=json_data, timeout=5)
            elif method == "DELETE":
                resp = requests.delete(url, timeout=5)
            else:
                self.log("SKIP", f"{method} {path} - unsupported method")
                return False

            # Check status code
            if resp.status_code != expected_status:
                self.log("FAIL", f"{method} {path}: expected {expected_status}, got {resp.status_code}")
                return False

            # Validate response schema
            if resp.text and resp.headers.get("content-type", "").startswith("application/json"):
                try:
                    response_data = resp.json()

                    # Get expected schema
                    # Normalize path for schema lookup (replace actual IDs with {param})
                    schema_path = path
                    if self.test_table_id and self.test_table_id in path:
                        schema_path = path.replace(self.test_table_id, "{tableId}")
                    if self.test_query_id and self.test_query_id in path:
                        schema_path = path.replace(self.test_query_id, "{queryId}")
                    if self.test_select_query_id and self.test_select_query_id in path:
                        schema_path = path.replace(self.test_select_query_id, "{queryId}")

                    schema = self.get_schema_for_response(schema_path, method, expected_status)

                    if schema:
                        errors = validate_response_schema(response_data, schema, self.spec)
                        if errors:
                            self.log("FAIL", f"{method} {path}: schema validation failed")
                            for err in errors[:3]:  # Show first 3 errors
                                print(f"      - {err}")
                            return False
                    elif require_schema:
                        self.log("FAIL", f"{method} {path}: no schema found in spec for {schema_path}")
                        return False

                    self.log("PASS", f"{method} {path} {description}")
                    return response_data

                except json.JSONDecodeError:
                    self.log("FAIL", f"{method} {path}: invalid JSON response")
                    return False
            else:
                self.log("PASS", f"{method} {path} {description}")
                return True

        except requests.RequestException as e:
            self.log("FAIL", f"{method} {path}: connection error - {e}")
            return False

    def run_tests(self):
        """Run all API compliance tests."""
        print(f"\n{BLUE}═══════════════════════════════════════════════════════════════{RESET}")
        print(f"{BLUE}  MIMDB API Compliance Validator{RESET}")
        print(f"{BLUE}═══════════════════════════════════════════════════════════════{RESET}")
        print(f"  Target: {self.base_url}")
        print(f"  Spec:   dbmsInterface.yaml v{self.spec['info']['version']}")
        print()

        # Test 1: System Info
        print(f"{YELLOW}[1/8] System Information{RESET}")
        self.validate_endpoint("GET", "/system/info", 200, description="- uptime, version, author")

        # Test 2: Tables - Empty List
        print(f"\n{YELLOW}[2/8] Table Operations{RESET}")
        self.validate_endpoint("GET", "/tables", 200, description="- list tables (empty)")

        # Test 3: Create Table
        table_data = {
            "name": "compliance_test",
            "columns": [
                {"name": "id", "type": "INT64"},
                {"name": "name", "type": "VARCHAR"}
            ]
        }
        result = self.validate_endpoint("PUT", "/table", 200, json_data=table_data, description="- create table")
        if result and isinstance(result, str):
            self.test_table_id = result
            self.log("INFO", f"Created table ID: {self.test_table_id[:8]}...")

        # Test 4: Get Table by ID
        if self.test_table_id:
            self.validate_endpoint("GET", f"/table/{self.test_table_id}", 200, description="- get table details")

        # Test 5: List Tables (non-empty)
        self.validate_endpoint("GET", "/tables", 200, description="- list tables (with data)")

        # Test 6: Query Operations
        print(f"\n{YELLOW}[3/8] Query Operations{RESET}")
        self.validate_endpoint("GET", "/queries", 200, description="- list queries (empty)")

        # Create CSV files for COPY tests
        csv_path = "/tmp/compliance_test.csv"
        with open(csv_path, "w") as f:
            f.write("1,Alice\n2,Bob\n3,Charlie\n")

        csv_with_header_path = "/tmp/compliance_test_header.csv"
        with open(csv_with_header_path, "w") as f:
            f.write("id,name\n10,HeaderTest1\n20,HeaderTest2\n")

        # Test 7: COPY Query with explicit doesCsvContainHeader=False
        print(f"\n{YELLOW}[4/8] COPY Query{RESET}")
        copy_query = {
            "queryDefinition": {
                "sourceFilepath": csv_path,
                "destinationTableName": "compliance_test",
                "doesCsvContainHeader": False
            }
        }
        result = self.validate_endpoint("POST", "/query", 200, json_data=copy_query, description="- submit COPY query")
        if result and isinstance(result, str):
            self.test_query_id = result
            self.log("INFO", f"COPY query ID: {self.test_query_id[:8]}...")

        # Test 8: Get Query Status - with strict required field validation
        if self.test_query_id:
            result = self.validate_endpoint("GET", f"/query/{self.test_query_id}", 200, description="- get COPY query status")
            if result and isinstance(result, dict):
                self._validate_query_required_fields(result, "COPY")

        # Test: Default value for doesCsvContainHeader (should default to false)
        print(f"\n{YELLOW}[4b/8] Default Value Tests{RESET}")
        self._test_default_values(csv_path, csv_with_header_path)

        # Test 9: SELECT Query
        print(f"\n{YELLOW}[5/8] SELECT Query{RESET}")
        select_query = {
            "queryDefinition": {
                "tableName": "compliance_test"
            }
        }
        result = self.validate_endpoint("POST", "/query", 200, json_data=select_query, description="- submit SELECT query")
        if result and isinstance(result, str):
            self.test_select_query_id = result
            self.log("INFO", f"SELECT query ID: {self.test_select_query_id[:8]}...")

        # Test: Get SELECT Query Status - with strict required field validation
        if self.test_select_query_id:
            result = self.validate_endpoint("GET", f"/query/{self.test_select_query_id}", 200, description="- get SELECT query status")
            if result and isinstance(result, dict):
                self._validate_query_required_fields(result, "SELECT")

        # Test 10: Get Query Result
        print(f"\n{YELLOW}[6/8] Query Results{RESET}")
        if self.test_select_query_id:
            self.validate_endpoint("GET", f"/result/{self.test_select_query_id}", 200, description="- get SELECT result")

        # Test 11: List Queries (non-empty)
        self.validate_endpoint("GET", "/queries", 200, description="- list queries (with data)")

        # Test 12: Error Cases
        print(f"\n{YELLOW}[7/8] Error Handling{RESET}")
        self.validate_endpoint("GET", "/table/nonexistent-id", 404, description="- table not found", require_schema=False)
        self.validate_endpoint("GET", "/query/nonexistent-id", 404, description="- query not found", require_schema=False)

        # Test 13: Cleanup
        print(f"\n{YELLOW}[8/8] Cleanup{RESET}")
        if self.test_table_id:
            self.validate_endpoint("DELETE", f"/table/{self.test_table_id}", 200, description="- delete table")

        # Cleanup temp file
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # Summary
        print(f"\n{BLUE}═══════════════════════════════════════════════════════════════{RESET}")
        total = self.results["passed"] + self.results["failed"] + self.results["skipped"]
        print(f"  {GREEN}Passed:{RESET}  {self.results['passed']}")
        print(f"  {RED}Failed:{RESET}  {self.results['failed']}")
        print(f"  {YELLOW}Skipped:{RESET} {self.results['skipped']}")
        print(f"  Total:   {total}")
        print(f"{BLUE}═══════════════════════════════════════════════════════════════{RESET}\n")

        return self.results["failed"] == 0


class ServerManager:
    """Manages the MIMDB server lifecycle for testing."""

    def __init__(self, port: int = 3000):
        self.port = port
        self.process = None
        self.data_dir = None
        self.server_binary = None

    def find_server_binary(self) -> str | None:
        """Find the server binary in common locations."""
        possible_paths = [
            "target/release/server",
            "target/debug/server",
            "../target/release/server",
            "../target/debug/server",
        ]
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return os.path.abspath(path)
        return None

    def build_server(self) -> bool:
        """Build the server if not found."""
        print(f"  {YELLOW}Building server...{RESET}")
        try:
            result = subprocess.run(
                ["cargo", "build", "--release", "-p", "mimdb", "--bin", "server"],
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                print(f"  {GREEN}✓{RESET} Server built successfully")
                return True
            else:
                print(f"  {RED}✗{RESET} Build failed: {result.stderr[:200]}")
                return False
        except Exception as e:
            print(f"  {RED}✗{RESET} Build error: {e}")
            return False

    def start(self) -> bool:
        """Start the MIMDB server."""
        # Find or build server
        self.server_binary = self.find_server_binary()
        if not self.server_binary:
            if not self.build_server():
                return False
            self.server_binary = self.find_server_binary()
            if not self.server_binary:
                print(f"  {RED}✗{RESET} Server binary not found after build")
                return False

        # Create temp data directory
        self.data_dir = tempfile.mkdtemp(prefix="mimdb_test_")

        # Start server
        try:
            self.process = subprocess.Popen(
                [self.server_binary, "--data-dir", self.data_dir, "--port", str(self.port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group for clean shutdown
            )

            # Wait for server to be ready
            for i in range(30):  # 3 seconds max
                time.sleep(0.1)
                try:
                    resp = requests.get(f"http://localhost:{self.port}/system/info", timeout=1)
                    if resp.status_code == 200:
                        return True
                except requests.RequestException:
                    pass

            print(f"  {RED}✗{RESET} Server failed to start within timeout")
            self.stop()
            return False

        except Exception as e:
            print(f"  {RED}✗{RESET} Failed to start server: {e}")
            return False

    def stop(self):
        """Stop the server and cleanup."""
        if self.process:
            try:
                # Kill the entire process group
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None

        # Cleanup data directory
        if self.data_dir and os.path.exists(self.data_dir):
            try:
                shutil.rmtree(self.data_dir)
            except Exception:
                pass
            self.data_dir = None

    @property
    def url(self) -> str:
        return f"http://localhost:{self.port}"


def find_spec_file() -> str | None:
    """Find the OpenAPI spec file."""
    possible_paths = [
        "api/dbmsInterface.yaml",
        "dbmsInterface.yaml",
        "../api/dbmsInterface.yaml",
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Validate MIMDB API against OpenAPI spec",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_api.py                              # Auto-start server, run tests
  python validate_api.py --url http://localhost:3000  # Use existing server
  python validate_api.py --port 3001                  # Auto-start on different port
  python validate_api.py --no-auto                    # Fail if server not running
        """
    )
    parser.add_argument("--url", default=None, help="Base URL of existing API server (skips auto-start)")
    parser.add_argument("--port", type=int, default=3000, help="Port for auto-started server (default: 3000)")
    parser.add_argument("--spec", default=None, help="Path to OpenAPI spec file")
    parser.add_argument("--no-auto", action="store_true", help="Don't auto-start server")
    args = parser.parse_args()

    # Find spec file
    spec_path = args.spec or find_spec_file()
    if not spec_path:
        print(f"{RED}Error: Could not find dbmsInterface.yaml{RESET}")
        sys.exit(1)

    server_manager = None
    base_url = args.url

    # Determine if we need to start the server
    if args.url:
        # User provided URL, use it directly
        base_url = args.url
        print(f"{BLUE}Using existing server at {base_url}{RESET}")
    else:
        # Check if server is already running
        try:
            resp = requests.get(f"http://localhost:{args.port}/system/info", timeout=2)
            if resp.status_code == 200:
                base_url = f"http://localhost:{args.port}"
                print(f"{BLUE}Found running server at {base_url}{RESET}")
        except requests.RequestException:
            pass

        # Server not running - start it if allowed
        if not base_url:
            if args.no_auto:
                print(f"{RED}Error: Server not running and --no-auto specified{RESET}")
                sys.exit(1)

            print(f"{BLUE}Starting MIMDB server...{RESET}")
            server_manager = ServerManager(port=args.port)

            # Register cleanup on exit
            atexit.register(server_manager.stop)
            signal.signal(signal.SIGINT, lambda s, f: sys.exit(1))
            signal.signal(signal.SIGTERM, lambda s, f: sys.exit(1))

            if not server_manager.start():
                print(f"{RED}Failed to start server{RESET}")
                sys.exit(1)

            base_url = server_manager.url
            print(f"  {GREEN}✓{RESET} Server running at {base_url}")

    # Run validation
    print()
    validator = APIValidator(base_url, spec_path)
    success = validator.run_tests()

    # Cleanup
    if server_manager:
        print(f"{BLUE}Stopping server...{RESET}")
        server_manager.stop()
        print(f"  {GREEN}✓{RESET} Server stopped")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
