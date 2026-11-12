CREATE TABLE IF NOT EXISTS login_handoffs (
  code_hash text PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS login_handoffs_expiry ON login_handoffs(expires_at);
CREATE TABLE IF NOT EXISTS stripe_events (
  event_id text PRIMARY KEY,
  event_type text NOT NULL,
  subscription_id text,
  processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS referral_commissions (
  id bigserial PRIMARY KEY,
  user_id uuid UNIQUE REFERENCES users(id) ON DELETE SET NULL,
  referrer_id uuid REFERENCES users(id) ON DELETE SET NULL,
  payment_id text UNIQUE,
  amount numeric(10,2) NOT NULL CHECK(amount>=0),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS email_outbox (
  id bigserial PRIMARY KEY,
  recipient text NOT NULL,
  subject text NOT NULL,
  body text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz,
  last_error text
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_expires_at timestamptz;
ALTER TABLE users ADD COLUMN IF NOT EXISTS terms_accepted_at timestamptz;
ALTER TABLE users ADD COLUMN IF NOT EXISTS terms_version text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_status text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_cancel_at_period_end boolean;
ALTER TABLE users ADD COLUMN IF NOT EXISTS manual_access_expires_at timestamptz;
UPDATE users SET manual_access_expires_at=membership_expires_at WHERE membership_active=true AND stripe_subscription_id IS NULL;
UPDATE sessions SET token='sha256:'||encode(digest(token,'sha256'),'hex') WHERE token NOT LIKE 'sha256:%';
UPDATE password_resets SET token='sha256:'||encode(digest(token,'sha256'),'hex') WHERE token NOT LIKE 'sha256:%';
UPDATE users SET verification_token='sha256:'||encode(digest(verification_token,'sha256'),'hex'), verification_expires_at=now()+interval '48 hours' WHERE verification_token IS NOT NULL AND verification_token NOT LIKE 'sha256:%';
INSERT INTO referral_commissions(user_id,amount) SELECT id,0 FROM users WHERE membership_activated_at IS NOT NULL AND stripe_subscription_id IS NOT NULL ON CONFLICT(user_id) DO NOTHING;
CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_lower ON users(lower(email));
CREATE UNIQUE INDEX IF NOT EXISTS users_username_unique_lower ON users(lower(username));
