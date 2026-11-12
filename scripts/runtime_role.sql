-- Run as the migration owner, never as the web application.
-- Provision the login secret privately in the database provider afterwards.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pugaxtrade_runtime') THEN
    CREATE ROLE pugaxtrade_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END $$;
GRANT USAGE ON SCHEMA public TO pugaxtrade_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pugaxtrade_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pugaxtrade_runtime;
REVOKE INSERT, UPDATE, DELETE ON schema_migrations FROM pugaxtrade_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pugaxtrade_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO pugaxtrade_runtime;
ALTER ROLE pugaxtrade_runtime SET statement_timeout='30s';
ALTER ROLE pugaxtrade_runtime SET lock_timeout='5s';
ALTER ROLE pugaxtrade_runtime SET idle_in_transaction_session_timeout='60s';
-- Do not grant ownership, membership in the owner role, or schema CREATE.
