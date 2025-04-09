-- Add migration script here
-- Create test tables
CREATE TABLE test_table(
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create test indexes
CREATE INDEX idx_test_table_name ON test_table(name(255));
CREATE INDEX idx_test_table_created_at ON test_table(created_at);
-- Insert test data
INSERT INTO test_table(name)
VALUES ('test1'),
  ('test2'),
  ('test3');