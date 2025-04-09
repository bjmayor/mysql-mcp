# MySQL MCP

MySQL MCP is a Model Context Protocol (MCP) implementation for MySQL databases. It provides a standardized interface for AI agents to interact with MySQL databases through a set of well-defined commands.

## Features

- **Connection Management**
  - Register and unregister database connections
  - Support for multiple concurrent database connections
  - Connection pooling for efficient resource management

- **Database Operations**
  - Execute SELECT queries
  - Insert new records
  - Update existing records
  - Delete records
  - Create and drop tables
  - Create and drop indexes
  - Describe table structures
  - List tables in a schema

- **SQL Validation**
  - Built-in SQL parser for validating statements
  - Support for MySQL-specific syntax
  - Safety checks to ensure only allowed operations are performed

## Installation

```bash
cargo install mysql-mcp
```

## Usage

### Configuration

Add the following to your MCP configuration file:

```json
{
  "mcpServers": {
    "mysql": {
      "command": "mysql-mcp",
      "args": ["stdio"]
    }
  }
}
```

or run it in SSE mode:

First, start the `mysql-mcp` server in SSE mode:

```bash
mysql-mcp sse
```

Then, configure the MCP config file to use the SSE mode:

```json
{
  "mcpServers": {
    "mysql": {
      "url": "http://localhost:3000/sse"
    }
  }
}
```

Once you started the `mysql-mcp` server, you should see the status of the MCP config is green, like this (cursor):

![mcp-status](./docs/images/mcp-status.jpg)

And then you could interact with it via the agent, like this (cursor):

![mcp](./docs/images/mcp.jpg)

### Commands

#### Register a Database Connection

```bash
mysql_mcp register "mysql://user:password@localhost:3306/database_name"
# Returns a connection ID (UUID)
```

#### Unregister a Connection

```bash
mysql_mcp unregister <connection_id>
```

#### Execute a SELECT Query

```bash
mysql_mcp query <connection_id> "SELECT * FROM users"
```

#### Insert Data

```bash
mysql_mcp insert <connection_id> "INSERT INTO users (name, email) VALUES ('John Doe', 'john.doe@example.com')"
```

#### Update Data

```bash
mysql_mcp update <connection_id> "UPDATE users SET name = 'Jane Doe' WHERE id = 1"
```

#### Delete Data

```bash
mysql_mcp delete <connection_id> "users" "1"
```

#### Create a Table

```bash
mysql_mcp create <connection_id> "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))"
```

#### Drop a Table

```bash
mysql_mcp drop <connection_id> "users"
```

#### Create an Index

```bash
mysql_mcp create_index <connection_id> "CREATE INDEX idx_users_name ON users (name)"
```

#### Drop an Index

```bash
mysql_mcp drop_index <connection_id> "idx_users_name"
```

#### Describe a Table

```bash
mysql_mcp describe <connection_id> "users"
```

## Dependencies

- Rust 1.70 or later
- MySQL 5.7 or later
- Required Rust crates:
  - anyhow: 1.0
  - arc-swap: 1.7
  - sqlx: 0.8 (with "runtime-tokio", "tls-rustls-aws-lc-rs", "mysql" features)
  - rmcp: 0.1 (with "server", "transport-sse-server", "transport-io" features)
  - schemars: 0.8
  - sqlparser: 0.55
  - tokio: 1.44

## Development

To build from source:

```bash
git clone https://github.com/bjmayor/mysql-mcp.git
cd mysql-mcp
cargo build --release
```

## License

MIT license. See [LICENSE.md](LICENSE.md) for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
