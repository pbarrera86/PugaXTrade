create extension if not exists pgcrypto;

create table if not exists users (
  id uuid primary key default gen_random_uuid(),
  username text unique,
  email text unique,
  name text,
  country text,
  phone text,
  password_hash text,
  is_admin boolean default false,
  created_at timestamptz default now(),
  trial_expires_at date,
  last_login_at timestamptz,
  active boolean default true,
  membership_active boolean default false,
  membership_activated_at timestamptz,
  membership_expires_at timestamptz,
  stripe_customer_id text,
  stripe_subscription_id text,
  referral_code text unique,
  referred_by text,
  referral_wallet numeric(10,2) default 0.00,
  email_verified boolean default false,
  verification_token text
);

create table if not exists password_resets (
  token text primary key,
  user_id uuid not null references users(id) on delete cascade,
  created_at timestamptz,
  expires_at timestamptz,
  used_at timestamptz
);

create table if not exists sessions (
  token text primary key,
  user_id uuid not null references users(id) on delete cascade,
  expires_at timestamptz not null
);

create index if not exists idx_users_email on users(lower(email));
create index if not exists idx_users_username on users(lower(username));
create index if not exists idx_sessions_user on sessions(user_id);
create index if not exists idx_sessions_expires on sessions(expires_at);

-- cached_analysis es la tabla activa de caché (ticker_cache era la versión anterior)
create table if not exists cached_analysis (
  ticker text primary key,
  data_json jsonb not null,
  last_updated timestamptz default now()
);

create index if not exists idx_cached_analysis_updated on cached_analysis(last_updated);

-- Rate limiting de login
create table if not exists login_attempts (
  id bigserial primary key,
  identifier text not null,
  attempted_at timestamptz default now(),
  success boolean default false
);
create index if not exists idx_login_attempts_identifier on login_attempts(identifier, attempted_at);

-- Migraciones para bases de datos existentes (seguras de re-ejecutar)
ALTER TABLE users ADD COLUMN IF NOT EXISTS membership_expires_at timestamptz;
ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code TEXT UNIQUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_wallet NUMERIC(10,2) DEFAULT 0.00;
ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_token TEXT;
