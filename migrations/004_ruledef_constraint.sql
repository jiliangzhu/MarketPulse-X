DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'rule_def_name_key'
    ) THEN
        ALTER TABLE rule_def ADD CONSTRAINT rule_def_name_key UNIQUE (name);
    END IF;
END;
$$;
