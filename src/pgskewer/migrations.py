from edwh_migrate import migration
from pydal import DAL


@migration()
def pgskewer_install_pgq_tables_001(db: DAL):
    # pgq install --dry-run
    db.executesql("""
    CREATE TYPE pgqueuer_status AS ENUM ('queued', 'picked', 'successful', 'exception', 'canceled', 'deleted');
    CREATE  TABLE pgqueuer (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        queue_manager_id UUID,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        execute_after TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        status pgqueuer_status NOT NULL,
        entrypoint TEXT NOT NULL,
        dedupe_key TEXT,
        payload BYTEA
    );
    CREATE INDEX pgqueuer_priority_id_id1_idx ON pgqueuer (priority ASC, id DESC)
        INCLUDE (id) WHERE status = 'queued';
    CREATE INDEX pgqueuer_updated_id_id1_idx ON pgqueuer (updated ASC, id DESC)
        INCLUDE (id) WHERE status = 'picked';
    CREATE INDEX pgqueuer_queue_manager_id_idx ON pgqueuer (queue_manager_id)
        WHERE queue_manager_id IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS pgqueuer_unique_dedupe_key ON
        pgqueuer (dedupe_key) WHERE ((status IN ('queued', 'picked') AND dedupe_key IS NOT NULL));

    CREATE  TABLE pgqueuer_log (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        job_id BIGINT NOT NULL,
        status pgqueuer_status NOT NULL,
        priority INT NOT NULL,
        entrypoint TEXT NOT NULL,
        traceback JSONB DEFAULT NULL,
        aggregated BOOLEAN DEFAULT FALSE
    );
    CREATE INDEX pgqueuer_log_not_aggregated ON pgqueuer_log ((1)) WHERE not aggregated;
    CREATE INDEX pgqueuer_log_created ON pgqueuer_log (created);
    CREATE INDEX pgqueuer_log_status ON pgqueuer_log (status);
    CREATE INDEX pgqueuer_log_job_id_status ON pgqueuer_log (job_id, created DESC);

    CREATE  TABLE pgqueuer_statistics (
        id SERIAL PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT DATE_TRUNC('sec', NOW() at time zone 'UTC'),
        count BIGINT NOT NULL,
        priority INT NOT NULL,
        status pgqueuer_status NOT NULL,
        entrypoint TEXT NOT NULL
    );
    CREATE UNIQUE INDEX pgqueuer_statistics_unique_count ON pgqueuer_statistics (
        priority,
        DATE_TRUNC('sec', created at time zone 'UTC'),
        status,
        entrypoint
    );

    CREATE  TABLE pgqueuer_schedules (
        id SERIAL PRIMARY KEY,
        expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
        entrypoint TEXT NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE,
        status pgqueuer_status DEFAULT 'queued',
        UNIQUE (expression, entrypoint)
    );

    CREATE FUNCTION fn_pgqueuer_changed() RETURNS TRIGGER AS $$
    DECLARE
        to_emit BOOLEAN := false;  -- Flag to decide whether to emit a notification
    BEGIN
        -- Check operation type and set the emit flag accordingly
        IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
            to_emit := true;
        ELSIF TG_OP = 'DELETE' THEN
            to_emit := true;
        ELSIF TG_OP = 'INSERT' THEN
            to_emit := true;
        ELSIF TG_OP = 'TRUNCATE' THEN
            to_emit := true;
        END IF;

        -- Perform notification if the emit flag is set
        IF to_emit THEN
            PERFORM pg_notify(
                'ch_pgqueuer',
                json_build_object(
                    'channel', 'ch_pgqueuer',
                    'operation', lower(TG_OP),
                    'sent_at', NOW(),
                    'table', TG_TABLE_NAME,
                    'type', 'table_changed_event'
                )::text
            );
        END IF;

        -- Return appropriate value based on the operation
        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NULL; -- For TRUNCATE and other non-row-specific contexts
        END IF;

    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER tg_pgqueuer_changed
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON pgqueuer
    EXECUTE FUNCTION fn_pgqueuer_changed();
    """)
    db.commit()
    return True


@migration()
def pgskewer_add_pgq_result_table_001(db: DAL):
    db.executesql("""
                  CREATE TABLE "pgqueuer_results"
                  (
                      id           SERIAL PRIMARY KEY,
                      job_id       BIGINT          NOT NULL,
                      entrypoint   TEXT            NOT NULL,
                      status       pgqueuer_status NOT NULL,
                      ok           BOOLEAN                  DEFAULT TRUE,
                      result       JSON            NOT NULL,
                      completed_at TIMESTAMP       NOT NULL DEFAULT NOW()
                  )
                  """)

    db.commit()
    return True


@migration()
def pgskewer_durability_001(db: DAL):
    # pgq durability --dry-run volatile
    # pgq autovac --dry-run

    db.executesql("""
                  ALTER TABLE pgqueuer
                      SET UNLOGGED;
                  ALTER TABLE pgqueuer_log
                      SET UNLOGGED;
                  ALTER TABLE pgqueuer_statistics
                      SET UNLOGGED;
                  ALTER TABLE pgqueuer_schedules
                      SET UNLOGGED;

                  ALTER TABLE pgqueuer
                      SET (
                          /* vacuum very early: after ~1 %% or 1 000 dead rows       */
                          AUTOVACUUM_VACUUM_SCALE_FACTOR = 0.01,
                          AUTOVACUUM_VACUUM_THRESHOLD = 1000,

                          /* analyse even earlier so the planner keeps up-to-date   */
                          AUTOVACUUM_ANALYZE_SCALE_FACTOR = 0.02,
                          AUTOVACUUM_ANALYZE_THRESHOLD = 500,

                          /* work fast: spend ≈10× the default IO budget            */
                          AUTOVACUUM_VACUUM_COST_LIMIT = 10000,
                          AUTOVACUUM_VACUUM_COST_DELAY = 0, -- no 20 ms naps

                      /* leave headroom for HOT updates so fewer dead tuples    */
                          FILLFACTOR = 70
                          );

                  ALTER TABLE pgqueuer_schedules
                      SET (
                          /* vacuum very early: after ~1 %% or 1 000 dead rows       */
                          AUTOVACUUM_VACUUM_SCALE_FACTOR = 0.01,
                          AUTOVACUUM_VACUUM_THRESHOLD = 1000,

                          /* analyse even earlier so the planner keeps up-to-date   */
                          AUTOVACUUM_ANALYZE_SCALE_FACTOR = 0.02,
                          AUTOVACUUM_ANALYZE_THRESHOLD = 500,

                          /* work fast: spend ≈10× the default IO budget            */
                          AUTOVACUUM_VACUUM_COST_LIMIT = 10000,
                          AUTOVACUUM_VACUUM_COST_DELAY = 0, -- no 20 ms naps

                      /* leave headroom for HOT updates so fewer dead tuples    */
                          FILLFACTOR = 70
                          );

                  ALTER TABLE pgqueuer_log
                      SET (
                          /* essentially disable vacuum (but keep freeze safety)    */
                          AUTOVACUUM_VACUUM_SCALE_FACTOR = 0.95,
                          AUTOVACUUM_VACUUM_THRESHOLD = 1000000,

                          /* still analyse at ~5 %% growth so estimates stay OK      */
                          AUTOVACUUM_ANALYZE_SCALE_FACTOR = 0.05,
                          AUTOVACUUM_ANALYZE_THRESHOLD = 10000
                          );

                  ALTER TABLE pgqueuer_statistics
                      SET (
                          /* essentially disable vacuum (but keep freeze safety)    */
                          AUTOVACUUM_VACUUM_SCALE_FACTOR = 0.95,
                          AUTOVACUUM_VACUUM_THRESHOLD = 1000000,

                          /* still analyse at ~5 %% growth so estimates stay OK      */
                          AUTOVACUUM_ANALYZE_SCALE_FACTOR = 0.05,
                          AUTOVACUUM_ANALYZE_THRESHOLD = 10000
                          );

                  """)
    db.commit()
    return True


@migration()
def pgskewer_pgqueuer_v0_24_headers_column_001(db: DAL):
    # https://github.com/janbjorge/pgqueuer/releases/tag/0.24.0
    db.executesql("""ALTER TABLE pgqueuer
        ADD COLUMN IF NOT EXISTS headers JSONB;""")
    db.commit()
    return True


def noop():
    """
    You just need to import this file, but if your editor complains that your import is useless,
    you can call `pgskewer.migrations.noop()`
    """
