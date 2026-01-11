/*
 * Copyright (c) 2026-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # MIMDB CLI
//!
//! Interactive command-line interface for the MIMDB database system.
//! Connects to a running MIMDB server and executes queries.

use rustyline::DefaultEditor;
use std::env;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_PORT: u16 = 3000;
const DEFAULT_HOST: &str = "localhost";

struct MimdbClient {
    base_url: String,
    client: reqwest::blocking::Client,
}

impl MimdbClient {
    fn new(host: &str, port: u16) -> Self {
        Self {
            base_url: format!("http://{}:{}", host, port),
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    fn get(&self, path: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}{}", self.base_url, path);
        match self.client.get(&url).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().map_err(|e| format!("JSON parse error: {}", e))
                } else {
                    Err(format!(
                        "HTTP {}: {}",
                        resp.status(),
                        resp.text().unwrap_or_default()
                    ))
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let url = format!("{}{}", self.base_url, path);
        match self.client.post(&url).json(body).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().map_err(|e| format!("JSON parse error: {}", e))
                } else {
                    Err(format!(
                        "HTTP {}: {}",
                        resp.status(),
                        resp.text().unwrap_or_default()
                    ))
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    fn put(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let url = format!("{}{}", self.base_url, path);
        match self.client.put(&url).json(body).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp.json().map_err(|e| format!("JSON parse error: {}", e))
                } else {
                    Err(format!(
                        "HTTP {}: {}",
                        resp.status(),
                        resp.text().unwrap_or_default()
                    ))
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    fn delete(&self, path: &str) -> Result<(), String> {
        let url = format!("{}{}", self.base_url, path);
        match self.client.delete(&url).send() {
            Ok(resp) => {
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(format!(
                        "HTTP {}: {}",
                        resp.status(),
                        resp.text().unwrap_or_default()
                    ))
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    fn wait_for_query(&self, query_id: &str) -> Result<(), String> {
        loop {
            let query = self.get(&format!("/query/{}", query_id))?;
            let status = query["status"].as_str().unwrap_or("");
            match status {
                "COMPLETED" => return Ok(()),
                "FAILED" => {
                    // Try to fetch detailed errors from the server
                    match self.get(&format!("/error/{}", query_id)) {
                        Ok(err_json) => {
                            let messages: Vec<String> = err_json["problems"]
                                .as_array()
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|p| p["error"].as_str().map(|s| s.to_string()))
                                        .collect()
                                })
                                .unwrap_or_default();

                            if messages.is_empty() {
                                return Err("Query failed".to_string());
                            } else {
                                return Err(format!("Query failed: {}", messages.join("; ")));
                            }
                        }
                        Err(e) => {
                            return Err(format!(
                                "Query failed (could not fetch error details): {}",
                                e
                            ));
                        }
                    }
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }
}

fn print_help() {
    println!(
        r#"
MIMDB CLI Commands:
==================

  SYSTEM:
    info                    - Show system information
    help                    - Show this help message
    exit, quit              - Exit the CLI

  TABLES:
    tables                  - List all tables
    describe <table>        - Show table schema
    create <table> with <columns>
                            - Create a new table
                              Example: create users with id:INT64 name:VARCHAR
    drop <table>            - Delete a table

  DATA:
    copy <file> to <table> [into columns] [with header]
                            - Load CSV data into a table
                              Example: copy data.csv to users
                              Example: copy data.csv to users into id,name
                              Example: copy data.csv to users with header

  QUERIES:
    queries                 - List all queries
    query <id>              - Get query status
    result <id>             - Get query result

  SELECT:
    select <columns> from <table> [where <expr>] [order by <col> [asc|desc], ...] [limit <n>]
                            - Execute a SELECT query
                              Examples:
                              select * from users
                              select id,name from users
                              select id,name from users where id > 5
                              select * from users order by name
                              select * from users order by age desc, name asc
                              select * from users limit 10

  EXPRESSIONS:
    Arithmetic:             +, -, *, /
    Comparison:             =, !=, <, <=, >, >=
    Logical:                AND, OR
    Functions:              upper(x), lower(x), strlen(x), concat(a, b)
    Literals:               123, "text", 'text', true, false

                            Examples:
                              select upper(name) from users
                              select concat(first, last) from users
                              select strlen(name) from users where id > 5
                              select id * 10 from users

  RAW:
    raw <json>              - Execute raw JSON query
"#
    );
}

fn parse_column_type(type_str: &str) -> Option<&'static str> {
    match type_str.to_uppercase().as_str() {
        "INT64" | "INT" | "INTEGER" => Some("INT64"),
        "VARCHAR" | "STRING" | "TEXT" => Some("VARCHAR"),
        "BOOL" | "BOOLEAN" => Some("BOOL"),
        _ => None,
    }
}

fn handle_create_table(client: &MimdbClient, args: &[&str]) {
    if args.len() < 3 {
        println!("Usage: create <table_name> with <col1:type> [col2:type] ...");
        println!("Types: INT64, VARCHAR, BOOL");
        return;
    }

    if args[1].to_lowercase() != "with" {
        println!("✗ Expected 'WITH' keyword at position 2");
        println!("Usage: create <table_name> with <col1:type> [col2:type] ...");
        return;
    }

    let table_name = args[0];
    let col_defs = &args[2..];
    let mut columns = Vec::new();

    for col_def in col_defs {
        let parts: Vec<&str> = col_def.split(':').collect();
        if parts.len() != 2 {
            println!(
                "Invalid column definition '{}'. Use format: name:type",
                col_def
            );
            return;
        }
        let col_name = parts[0];
        let col_type = match parse_column_type(parts[1]) {
            Some(t) => t,
            None => {
                println!("Unknown type '{}'. Use INT64, VARCHAR, or BOOL", parts[1]);
                return;
            }
        };
        columns.push(serde_json::json!({
            "name": col_name,
            "type": col_type
        }));
    }

    let body = serde_json::json!({
        "name": table_name,
        "columns": columns
    });

    match client.put("/table", &body) {
        Ok(result) => {
            println!(
                "✓ Table '{}' created with ID: {}",
                table_name, result["tableId"]
            );
        }
        Err(e) => println!("✗ Error: {}", e),
    }
}

fn handle_copy(client: &MimdbClient, args: &[&str]) {
    if args.len() < 3 {
        println!("Usage: copy <csv_file> to <table_name> [into columns] [with header]");
        return;
    }

    if args[1].to_lowercase() != "to" {
        println!("✗ Expected 'TO' keyword at position 2");
        println!("Usage: copy <csv_file> to <table_name> [into columns] [with header]");
        return;
    }

    let file_path = args[0];
    let table_name = args[2];

    // Parse optional keywords
    let mut columns: Option<Vec<&str>> = None;
    let mut has_header = false;

    let mut i = 3;
    while i < args.len() {
        match args[i].to_lowercase().as_str() {
            "into" => {
                if i + 1 < args.len() {
                    columns = Some(args[i + 1].split(',').collect());
                    i += 2;
                } else {
                    println!("✗ No columns specified after INTO");
                    return;
                }
            }
            "with" => {
                if i + 1 < args.len() && args[i + 1].to_lowercase() == "header" {
                    has_header = true;
                    i += 2;
                } else {
                    println!("✗ Expected 'header' after WITH");
                    return;
                }
            }
            _ => {
                println!("✗ Unknown keyword: {}", args[i]);
                return;
            }
        }
    }

    let mut query_def = serde_json::json!({
        "sourceFilepath": file_path,
        "destinationTableName": table_name,
        "doesCsvContainHeader": has_header
    });

    if let Some(cols) = columns {
        query_def["destinationColumns"] = serde_json::json!(cols);
    }

    let body = serde_json::json!({
        "queryDefinition": query_def
    });

    match client.post("/query", &body) {
        Ok(result) => {
            let query_id = result.as_str().unwrap_or("");
            println!("✓ COPY query submitted: {}", query_id);
            print!("  Waiting for completion...");
            io::stdout().flush().unwrap();
            match client.wait_for_query(query_id) {
                Ok(()) => println!(" done!"),
                Err(e) => println!(" {}", e),
            }
        }
        Err(e) => println!("✗ Error: {}", e),
    }
}

/// Find the matching closing parenthesis for an opening one at position `open_pos`
fn find_matching_paren(s: &str, open_pos: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = b'"';

    for (i, &ch) in bytes.iter().enumerate().skip(open_pos) {
        if !in_string {
            if ch == b'"' || ch == b'\'' {
                in_string = true;
                string_char = ch;
            } else if ch == b'(' {
                depth += 1;
            } else if ch == b')' {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        } else if ch == string_char {
            in_string = false;
        }
    }
    None
}

/// Split arguments by comma, respecting parentheses and strings
fn split_function_args(args_str: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = b'"';
    let mut start = 0;
    let bytes = args_str.as_bytes();

    for (i, &ch) in bytes.iter().enumerate() {
        if !in_string {
            if ch == b'"' || ch == b'\'' {
                in_string = true;
                string_char = ch;
            } else if ch == b'(' {
                depth += 1;
            } else if ch == b')' {
                depth -= 1;
            } else if ch == b',' && depth == 0 {
                result.push(args_str[start..i].trim());
                start = i + 1;
            }
        } else if ch == string_char {
            in_string = false;
        }
    }
    // Push the last argument
    let last = args_str[start..].trim();
    if !last.is_empty() {
        result.push(last);
    }
    result
}

fn build_column_expression(table: &str, expr: &str) -> serde_json::Value {
    let expr = expr.trim();

    // Check for logical operators first (lowest precedence)
    let expr_upper = expr.to_uppercase();
    if let Some(pos) = expr_upper.find(" OR ") {
        let left = &expr[..pos].trim();
        let right = &expr[pos + 4..].trim();
        return serde_json::json!({
            "operator": "OR",
            "leftOperand": build_column_expression(table, left),
            "rightOperand": build_column_expression(table, right)
        });
    }
    if let Some(pos) = expr_upper.find(" AND ") {
        let left = &expr[..pos].trim();
        let right = &expr[pos + 5..].trim();
        return serde_json::json!({
            "operator": "AND",
            "leftOperand": build_column_expression(table, left),
            "rightOperand": build_column_expression(table, right)
        });
    }

    // Check for comparison operators (outside of parentheses/strings)
    let comparison_ops = [">=", "<=", "!=", "=", ">", "<"];
    for op in comparison_ops {
        if let Some(pos) = find_operator_position(expr, op) {
            let left = expr[..pos].trim();
            let right = expr[pos + op.len()..].trim();
            let operator = match op {
                ">=" => "GREATER_EQUAL",
                "<=" => "LESS_EQUAL",
                "!=" => "NOT_EQUAL",
                "=" => "EQUAL",
                ">" => "GREATER_THAN",
                "<" => "LESS_THAN",
                _ => unreachable!(),
            };
            return serde_json::json!({
                "operator": operator,
                "leftOperand": build_column_expression(table, left),
                "rightOperand": build_column_expression(table, right)
            });
        }
    }

    // Check for arithmetic operators (outside of parentheses/strings)
    let arithmetic_ops = ["+", "-", "*", "/"];
    for op in arithmetic_ops {
        if let Some(pos) = find_operator_position(expr, op) {
            let left = expr[..pos].trim();
            let right = expr[pos + op.len()..].trim();
            let operator = match op {
                "+" => "ADD",
                "-" => "SUBTRACT",
                "*" => "MULTIPLY",
                "/" => "DIVIDE",
                _ => unreachable!(),
            };
            return serde_json::json!({
                "operator": operator,
                "leftOperand": build_column_expression(table, left),
                "rightOperand": build_column_expression(table, right)
            });
        }
    }

    // Check for function calls: name(args)
    // Supported: upper, lower, strlen, concat
    // Only parse as function if the entire expression is a function call
    if let Some(paren_pos) = expr.find('(') {
        let func_name = expr[..paren_pos].trim().to_uppercase();
        if matches!(func_name.as_str(), "UPPER" | "LOWER" | "STRLEN" | "CONCAT")
            && let Some(close_pos) = find_matching_paren(expr, paren_pos)
            && close_pos == expr.len() - 1
        {
            let args_str = &expr[paren_pos + 1..close_pos];
            let args = split_function_args(args_str);
            let arguments: Vec<serde_json::Value> = args
                .iter()
                .map(|arg| build_column_expression(table, arg))
                .collect();
            return serde_json::json!({
                "functionName": func_name,
                "arguments": arguments
            });
        }
    }

    // Try parsing as literal
    if let Ok(num) = expr.parse::<i64>() {
        return serde_json::json!({ "value": num });
    }
    if expr == "true" {
        return serde_json::json!({ "value": true });
    }
    if expr == "false" {
        return serde_json::json!({ "value": false });
    }
    if expr.starts_with('"') && expr.ends_with('"') {
        return serde_json::json!({ "value": &expr[1..expr.len()-1] });
    }
    if expr.starts_with('\'') && expr.ends_with('\'') {
        return serde_json::json!({ "value": &expr[1..expr.len()-1] });
    }

    // Default: column reference
    serde_json::json!({
        "tableName": table,
        "columnName": expr
    })
}

/// Find an operator position that is not inside parentheses or strings
fn find_operator_position(expr: &str, op: &str) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = b'"';
    let bytes = expr.as_bytes();
    let op_bytes = op.as_bytes();

    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if !in_string {
            if ch == b'"' || ch == b'\'' {
                in_string = true;
                string_char = ch;
            } else if ch == b'(' {
                depth += 1;
            } else if ch == b')' {
                depth -= 1;
            } else if depth == 0
                && i + op_bytes.len() <= bytes.len()
                && &bytes[i..i + op_bytes.len()] == op_bytes
            {
                return Some(i);
            }
        } else if ch == string_char {
            in_string = false;
        }
        i += 1;
    }
    None
}

fn handle_select(client: &MimdbClient, args: &[&str]) {
    if args.len() < 3 {
        println!(
            "Usage: select <columns> from <table> [where <expr>] [order <col> <asc|desc>] [limit <n>]"
        );
        return;
    }

    // Find "from" keyword to split columns and table
    let from_idx = match args.iter().position(|&arg| arg.to_lowercase() == "from") {
        Some(idx) => idx,
        None => {
            println!("✗ Missing 'FROM' keyword");
            println!(
                "Usage: select <columns> from <table> [where <expr>] [order <col> <asc|desc>] [limit <n>]"
            );
            return;
        }
    };

    if from_idx == 0 {
        println!("✗ No columns specified before FROM");
        return;
    }

    if from_idx + 1 >= args.len() {
        println!("✗ No table specified after FROM");
        return;
    }

    // Collect all tokens between SELECT and FROM as the columns expression
    let columns_str = args[0..from_idx].join(" ");
    let table_name = args[from_idx + 1];

    // First, get table schema to know column names if * is used
    let table_columns: Vec<String> = if columns_str == "*" {
        // Need to find table and get its columns
        match client.get("/tables") {
            Ok(tables) => {
                if let Some(arr) = tables.as_array() {
                    let table_entry = arr.iter().find(|t| t["name"].as_str() == Some(table_name));
                    if let Some(entry) = table_entry {
                        let table_id = entry["tableId"].as_str().unwrap_or("");
                        match client.get(&format!("/table/{}", table_id)) {
                            Ok(schema) => schema["columns"]
                                .as_array()
                                .unwrap_or(&vec![])
                                .iter()
                                .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
                                .collect(),
                            Err(_) => {
                                println!("✗ Could not get table schema");
                                return;
                            }
                        }
                    } else {
                        println!("✗ Table '{}' not found", table_name);
                        return;
                    }
                } else {
                    println!("✗ Could not list tables");
                    return;
                }
            }
            Err(e) => {
                println!("✗ Error: {}", e);
                return;
            }
        }
    } else {
        // Split by comma to get individual column expressions
        columns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };

    // Build column clauses - use build_column_expression to parse expressions
    let column_clauses: Vec<serde_json::Value> = table_columns
        .iter()
        .map(|col| build_column_expression(table_name, col))
        .collect();

    let mut query_def = serde_json::json!({
        "columnClauses": column_clauses
    });

    // Parse optional clauses starting after the table name
    let mut i = from_idx + 2;
    while i < args.len() {
        match args[i].to_lowercase().as_str() {
            "where" => {
                if i + 1 < args.len() {
                    // Collect all parts until next keyword
                    let mut where_parts = Vec::new();
                    i += 1;
                    while i < args.len()
                        && !["order", "limit"].contains(&args[i].to_lowercase().as_str())
                    {
                        where_parts.push(args[i]);
                        i += 1;
                    }
                    let where_expr = where_parts.join(" ");
                    query_def["whereClause"] = build_column_expression(table_name, &where_expr);
                }
            }
            "order" => {
                // Check for "order by" syntax
                if i + 1 < args.len() && args[i + 1].to_lowercase() == "by" {
                    if i + 2 < args.len() {
                        let mut order_exprs = Vec::new();
                        i += 2; // Skip "order by"

                        // Parse columns: col1 [asc|desc], col2 [asc|desc], ...
                        while i < args.len() {
                            let col_part = args[i];
                            // Check if column name ends with comma
                            let col_name = col_part.trim_end_matches(',');
                            let has_trailing_comma = col_part.ends_with(',');

                            // Find column index
                            let col_index = table_columns
                                .iter()
                                .position(|c| c == col_name)
                                .unwrap_or(0);

                            // Check if next argument is asc/desc
                            let mut ascending = true;
                            i += 1;

                            if i < args.len() {
                                let next_arg = args[i].to_lowercase();
                                let next_trimmed = next_arg.trim_end_matches(',');
                                match next_trimmed {
                                    "asc" => {
                                        ascending = true;
                                        let has_comma = args[i].ends_with(',');
                                        i += 1;
                                        if !has_comma && i < args.len() && args[i] == "," {
                                            i += 1;
                                        }
                                    }
                                    "desc" => {
                                        ascending = false;
                                        let has_comma = args[i].ends_with(',');
                                        i += 1;
                                        if !has_comma && i < args.len() && args[i] == "," {
                                            i += 1;
                                        }
                                    }
                                    "limit" => {
                                        // Next keyword, stop parsing order by
                                        order_exprs.push(serde_json::json!({
                                            "columnIndex": col_index,
                                            "ascending": ascending
                                        }));
                                        break;
                                    }
                                    _ => {
                                        // Not asc/desc, might be next column or keyword
                                        if !has_trailing_comma {
                                            order_exprs.push(serde_json::json!({
                                                "columnIndex": col_index,
                                                "ascending": ascending
                                            }));
                                            break;
                                        }
                                    }
                                }
                            }

                            order_exprs.push(serde_json::json!({
                                "columnIndex": col_index,
                                "ascending": ascending
                            }));

                            if has_trailing_comma {
                                continue;
                            }
                        }

                        if !order_exprs.is_empty() {
                            query_def["orderByClause"] = serde_json::json!(order_exprs);
                        }
                    } else {
                        println!("✗ No column specified after ORDER BY");
                        return;
                    }
                } else {
                    println!("✗ Expected 'BY' after ORDER");
                    return;
                }
            }
            "limit" => {
                if i + 1 < args.len() {
                    if let Ok(limit) = args[i + 1].parse::<i32>() {
                        query_def["limitClause"] = serde_json::json!({ "limit": limit });
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let body = serde_json::json!({
        "queryDefinition": query_def
    });

    match client.post("/query", &body) {
        Ok(result) => {
            let query_id = result.as_str().unwrap_or("");
            print!("Executing query {}...", query_id);
            io::stdout().flush().unwrap();

            match client.wait_for_query(query_id) {
                Ok(()) => {
                    println!(" done!");
                    // Get and display result
                    match client.get(&format!("/result/{}", query_id)) {
                        Ok(result) => {
                            print_result(&table_columns, &result);
                        }
                        Err(e) => println!("✗ Error getting result: {}", e),
                    }
                }
                Err(e) => println!(" {}", e),
            }
        }
        Err(e) => println!("✗ Error: {}", e),
    }
}

fn print_result(columns: &[String], result: &serde_json::Value) {
    if let Some(batches) = result.as_array() {
        let mut total_rows = 0;

        for batch in batches {
            let row_count = batch["rowCount"].as_i64().unwrap_or(0) as usize;
            total_rows += row_count;

            if let Some(cols_data) = batch["columns"].as_array() {
                // Print header
                if total_rows == row_count {
                    println!();
                    for (i, col_name) in columns.iter().enumerate() {
                        if i > 0 {
                            print!(" | ");
                        }
                        print!("{:>15}", col_name);
                    }
                    println!();
                    println!("{}", "-".repeat(columns.len() * 18));
                }

                // Print rows
                for row_idx in 0..row_count {
                    for (col_idx, col_data) in cols_data.iter().enumerate() {
                        if col_idx > 0 {
                            print!(" | ");
                        }
                        if let Some(arr) = col_data.as_array() {
                            if row_idx < arr.len() {
                                let val = &arr[row_idx];
                                let s = if val.is_string() {
                                    val.as_str().unwrap_or("").to_string()
                                } else {
                                    val.to_string()
                                };
                                print!("{:>15}", s);
                            } else {
                                print!("{:>15}", "NULL");
                            }
                        }
                    }
                    println!();
                }
            }
        }

        println!("{}", "-".repeat(columns.len() * 18));
        println!("({} rows)", total_rows);
    }
}

fn handle_raw_query(client: &MimdbClient, args: &[&str]) {
    if args.is_empty() {
        println!("Usage: raw <json>");
        return;
    }

    let json_str = args.join(" ");
    match serde_json::from_str::<serde_json::Value>(&json_str) {
        Ok(body) => match client.post("/query", &body) {
            Ok(result) => {
                println!("Query submitted: {}", result);
            }
            Err(e) => println!("✗ Error: {}", e),
        },
        Err(e) => println!("✗ Invalid JSON: {}", e),
    }
}

fn run_cli(client: &MimdbClient) {
    println!("MIMDB CLI - Connected to {}", client.base_url);
    println!("Type 'help' for available commands, 'exit' to quit.\n");

    // Initialize history
    let history_file = PathBuf::from(format!(
        "{}/.mimdb_history",
        env::var("HOME").unwrap_or_else(|_| ".".to_string())
    ));
    let mut editor = match DefaultEditor::new() {
        Ok(ed) => ed,
        Err(_) => {
            eprintln!("Warning: Failed to create editor with history");
            panic!()
        }
    };

    // Load existing history
    let _ = editor.load_history(&history_file);

    loop {
        let readline = editor.readline("mimdb> ");

        match readline {
            Ok(input) => {
                let input = input.trim();
                if input.is_empty() {
                    continue;
                }

                // Add to history
                let _ = editor.add_history_entry(input);

                let parts: Vec<&str> = input.split_whitespace().collect();
                let command = parts[0].to_lowercase();
                let args = &parts[1..];

                match command.as_str() {
                    "exit" | "quit" | "q" => {
                        println!("Goodbye!");
                        break;
                    }
                    "help" | "h" | "?" => print_help(),
                    "info" => match client.get("/system/info") {
                        Ok(info) => {
                            println!("System Information:");
                            println!("  Version: {}", info["version"]);
                            println!("  Author:  {}", info["author"]);
                            println!("  Uptime:  {}s", info["uptimeInSeconds"]);
                        }
                        Err(e) => println!("✗ Error: {}", e),
                    },
                    "tables" => match client.get("/tables") {
                        Ok(tables) => {
                            if let Some(arr) = tables.as_array() {
                                if arr.is_empty() {
                                    println!("No tables found.");
                                } else {
                                    println!("{:<40} NAME", "TABLE_ID");
                                    println!("{}", "-".repeat(60));
                                    for table in arr {
                                        println!(
                                            "{:<40} {}",
                                            table["tableId"].as_str().unwrap_or(""),
                                            table["name"].as_str().unwrap_or("")
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => println!("✗ Error: {}", e),
                    },
                    "describe" => {
                        if args.is_empty() {
                            println!("Usage: describe <table_name>");
                        } else {
                            // First find table by name
                            match client.get("/tables") {
                                Ok(tables) => {
                                    if let Some(arr) = tables.as_array() {
                                        let table = arr
                                            .iter()
                                            .find(|t| t["name"].as_str() == Some(args[0]));
                                        if let Some(t) = table {
                                            let table_id = t["tableId"].as_str().unwrap_or("");
                                            match client.get(&format!("/table/{}", table_id)) {
                                                Ok(schema) => {
                                                    println!("Table: {}", schema["name"]);
                                                    println!("ID: {}", table_id);
                                                    println!("\nColumns:");
                                                    println!("{:<20} TYPE", "NAME");
                                                    println!("{}", "-".repeat(40));
                                                    if let Some(cols) = schema["columns"].as_array()
                                                    {
                                                        for col in cols {
                                                            println!(
                                                                "{:<20} {}",
                                                                col["name"].as_str().unwrap_or(""),
                                                                col["type"].as_str().unwrap_or("")
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => println!("✗ Error: {}", e),
                                            }
                                        } else {
                                            println!("✗ Table '{}' not found", args[0]);
                                        }
                                    }
                                }
                                Err(e) => println!("✗ Error: {}", e),
                            }
                        }
                    }
                    "create" => handle_create_table(client, args),
                    "drop" => {
                        if args.is_empty() {
                            println!("Usage: drop <table_name>");
                        } else {
                            let table_name = args[0];
                            // First find table by name
                            match client.get("/tables") {
                                Ok(tables) => {
                                    if let Some(arr) = tables.as_array() {
                                        let table = arr
                                            .iter()
                                            .find(|t| t["name"].as_str() == Some(table_name));
                                        if let Some(t) = table {
                                            let table_id = t["tableId"].as_str().unwrap_or("");
                                            match client.delete(&format!("/table/{}", table_id)) {
                                                Ok(()) => {
                                                    println!("✓ Table '{}' deleted", table_name)
                                                }
                                                Err(e) => println!("✗ Error: {}", e),
                                            }
                                        } else {
                                            println!("✗ Table '{}' not found", table_name);
                                        }
                                    }
                                }
                                Err(e) => println!("✗ Error: {}", e),
                            }
                        }
                    }
                    "copy" => handle_copy(client, args),
                    "queries" => match client.get("/queries") {
                        Ok(queries) => {
                            if let Some(arr) = queries.as_array() {
                                if arr.is_empty() {
                                    println!("No queries found.");
                                } else {
                                    println!("{:<40} STATUS", "QUERY_ID");
                                    println!("{}", "-".repeat(60));
                                    for query in arr {
                                        println!(
                                            "{:<40} {}",
                                            query["queryId"].as_str().unwrap_or(""),
                                            query["status"].as_str().unwrap_or("")
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => println!("✗ Error: {}", e),
                    },
                    "query" => {
                        if args.is_empty() {
                            println!("Usage: query <query_id>");
                        } else {
                            match client.get(&format!("/query/{}", args[0])) {
                                Ok(query) => {
                                    println!("{}", serde_json::to_string_pretty(&query).unwrap());
                                }
                                Err(e) => println!("✗ Error: {}", e),
                            }
                        }
                    }
                    "result" => {
                        if args.is_empty() {
                            println!("Usage: result <query_id>");
                        } else {
                            match client.get(&format!("/result/{}", args[0])) {
                                Ok(result) => {
                                    println!("{}", serde_json::to_string_pretty(&result).unwrap());
                                }
                                Err(e) => println!("✗ Error: {}", e),
                            }
                        }
                    }
                    "select" => handle_select(client, args),
                    "raw" => handle_raw_query(client, args),
                    _ => println!(
                        "Unknown command '{}'. Type 'help' for available commands.",
                        command
                    ),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted)
            | Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                break;
            }
        }
    }

    // Save history on exit
    let _ = editor.save_history(&history_file);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut host = DEFAULT_HOST.to_string();
    let mut port = DEFAULT_PORT;

    // Parse command line arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --host requires a value");
                    std::process::exit(1);
                }
            }
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().unwrap_or(DEFAULT_PORT);
                    i += 2;
                } else {
                    eprintln!("Error: --port requires a value");
                    std::process::exit(1);
                }
            }
            "--help" => {
                println!("MIMDB CLI - Command Line Interface for MIMDB");
                println!();
                println!("Usage: cli [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -h, --host <HOST>  Server hostname (default: localhost)");
                println!(
                    "  -p, --port <PORT>  Server port (default: {})",
                    DEFAULT_PORT
                );
                println!("      --help         Show this help message");
                std::process::exit(0);
            }
            _ => i += 1,
        }
    }

    let client = MimdbClient::new(&host, port);

    // Test connection
    print!("Connecting to {}:{}...", host, port);
    io::stdout().flush().unwrap();

    match client.get("/system/info") {
        Ok(_) => {
            println!(" connected!");
            run_cli(&client);
        }
        Err(e) => {
            println!(" failed!");
            eprintln!("Could not connect to server: {}", e);
            eprintln!("Make sure the MIMDB server is running on port {}.", port);
            std::process::exit(1);
        }
    }
}
