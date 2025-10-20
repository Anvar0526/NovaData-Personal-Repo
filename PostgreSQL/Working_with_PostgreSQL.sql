CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

--1) Создал функцию для Триггера 
CREATE OR REPLACE FUNCTION log_user_audit()
RETURNS TRIGGER AS $$
BEGIN
	IF (TG_OP = 'UPDATE') THEN
	    IF OLD.name IS DISTINCT FROM NEW.name THEN
	        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
	        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
	    END IF; 
	
		IF OLD.email IS DISTINCT FROM NEW.email THEN 
			INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES(OLD.id, current_user, 'email', OLD.email, NEW.email);
		END IF;
		
		IF OLD.role IS DISTINCT FROM NEW.role THEN
			INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES(OLD.id, current_user, 'role', OLD.role, NEW.role);
		END IF;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;  

--2) Прикрутил триггер к ранее созданной функции
CREATE TRIGGER log_user_audit
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_audit();

--3 Добавили расширение Крон (для галочки, так как создан при разворачивании контейнера)
CREATE EXTENSION IF NOT exists pg_cron


--4 Функция для экспорта данных в CSV
CREATE OR REPLACE FUNCTION export_todays_audit_to_csv()
RETURNS void AS $$
DECLARE
    export_file_path TEXT;
    export_date TEXT;
BEGIN
    export_date := to_char(CURRENT_DATE, 'YYYYMMDD');
    export_file_path := '/tmp/users_audit_export_' || export_date || '.csv';
    EXECUTE format('
        COPY (
            SELECT user_id, changed_at, changed_by, field_changed, old_value, new_value
            FROM users_audit 
            WHERE changed_at::date = CURRENT_DATE
            ORDER BY changed_at
        ) TO %L WITH CSV HEADER', export_file_path);
    
    RAISE NOTICE 'Audit data exported to: %', export_file_path;
END;
$$ LANGUAGE plpgsql;


-- 5. Настройка планировщика pg_cron 
SELECT cron.schedule(
    'export-daily-audit',    
    '0 3 * * *',             
    'SELECT export_todays_audit_to_csv();'  
);


SELECT * FROM cron.job;