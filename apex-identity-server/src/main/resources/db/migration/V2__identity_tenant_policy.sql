CREATE TABLE identity_tenant (tenant_id UUID PRIMARY KEY);
CREATE TABLE identity_user (
  user_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES identity_tenant(tenant_id),
  login_name VARCHAR(100) NOT NULL CHECK (login_name ~ '^[a-zA-Z0-9._-]{1,100}$'),
  password_hash VARCHAR(255) NOT NULL CHECK (password_hash LIKE '{bcrypt}%'),
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (tenant_id, login_name)
);
CREATE FUNCTION identity_immutable_subject() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.user_id <> OLD.user_id OR NEW.tenant_id <> OLD.tenant_id THEN
    RAISE EXCEPTION 'Identity subject and tenant are immutable';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER identity_immutable_subject BEFORE UPDATE ON identity_user
FOR EACH ROW EXECUTE FUNCTION identity_immutable_subject();
ALTER TABLE oauth2_registered_client ADD CONSTRAINT uq_identity_client UNIQUE(client_id);
CREATE TABLE identity_client_policy (
  registered_client_id VARCHAR(100) PRIMARY KEY REFERENCES oauth2_registered_client(id),
  tenant_id UUID NOT NULL REFERENCES identity_tenant(tenant_id),
  audience VARCHAR(200) NOT NULL CHECK (audience ~ '^[a-zA-Z0-9:._/-]{1,200}$')
);
ALTER TABLE oauth2_authorization ADD CONSTRAINT fk_authorization_client
FOREIGN KEY (registered_client_id) REFERENCES oauth2_registered_client(id);
ALTER TABLE oauth2_authorization_consent ADD CONSTRAINT fk_consent_client
FOREIGN KEY (registered_client_id) REFERENCES oauth2_registered_client(id);
CREATE INDEX identity_authorization_principal ON oauth2_authorization(principal_name);
CREATE INDEX identity_authorization_code ON oauth2_authorization USING hash(authorization_code_value);
CREATE INDEX identity_authorization_refresh ON oauth2_authorization USING hash(refresh_token_value);
