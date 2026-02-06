
-- Default Admin User (Password: admin123)
-- Hash generated using Argon2
INSERT INTO user (email, hashed_password, role, is_active) 
VALUES ('admin@example.com', '$argon2id$v=19$m=65536,t=3,p=4$sf...HASH_PLACEHOLDER...$', 'admin', TRUE)
ON CONFLICT (email) DO NOTHING;

-- Default Rules (Indonesian Context)
INSERT INTO scanrule (name, rule_type, pattern, score, entity_type, is_active, context_keywords) VALUES
('NIK', 'regex', '\b[1-9][0-9]{15}\b', 0.85, 'CUSTOM_ID', TRUE, '["nik", "ktp", "nomor induk"]'),
('NPWP', 'regex', '\b[0-9]{2}\.[0-9]{3}\.[0-9]{3}\.[0-9]-[0-9]{3}\.[0-9]{3}\b', 0.85, 'CUSTOM_ID', TRUE, '["npwp", "pajak"]'),
('Email', 'regex', '\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', 0.75, 'PII', TRUE, '["email", "surat elektronik"]'),
('Phone Number', 'regex', '\b(\+62|62|0)8[1-9][0-9]{6,9}\b', 0.75, 'PII', TRUE, '["hp", "telepon", "handphone"]');
