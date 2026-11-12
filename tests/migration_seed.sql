INSERT INTO users(id,username,email,active,email_verified) VALUES('00000000-0000-0000-0000-000000000003','legacy_fixture','legacy@example.invalid',true,true);
INSERT INTO sessions(token,user_id,expires_at) VALUES(repeat('a',128),'00000000-0000-0000-0000-000000000003',now()+interval '1 day');
INSERT INTO password_resets(token,user_id,created_at,expires_at) VALUES(repeat('b',96),'00000000-0000-0000-0000-000000000003',now(),now()+interval '1 hour');
