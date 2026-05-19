CREATE TABLE IF NOT EXISTS "track" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TIMESTAMP,
    "description" TEXT NOT NULL,
    "name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "instructor" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TIMESTAMP,
    "bio" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "specialty" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "learner" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TIMESTAMP,
    "company" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "course" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TIMESTAMP,
    "instructor_id" INTEGER NOT NULL,
    "level" INTEGER NOT NULL,
    "list_price_cents" INTEGER NOT NULL,
    "prerequisite_id" INTEGER NOT NULL,
    "published" BOOLEAN NOT NULL,
    "summary" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "track_id" INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "enrollment" (
    "uid" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP,
    "deleted_at" INTEGER NOT NULL DEFAULT 0,
    "version" INTEGER NOT NULL DEFAULT 1,
    "course_id" INTEGER NOT NULL,
    "fee_cents" INTEGER NOT NULL,
    "learner_id" INTEGER NOT NULL,
    "score" INTEGER NOT NULL,
    "status" INTEGER NOT NULL
);

INSERT INTO "track" ("id", "created_at", "name", "description") VALUES
    (1, '2026-01-01 09:00:00', 'Backend Engineering', 'Build production backend services, APIs, and data access layers.'),
    (2, '2026-01-01 09:00:00', 'Data & AI', 'Ship retrieval, ranking, and applied machine learning workflows.'),
    (3, '2026-01-01 09:00:00', 'Platform Reliability', 'Operate stable platforms, on-call systems, and internal automation.');

INSERT INTO "instructor" ("id", "created_at", "name", "email", "specialty", "bio") VALUES
    (1, '2026-01-01 09:00:00', 'Nora Patel', 'nora@academy.test', 'Backend Architecture', 'Designs high-throughput Go services and durable persistence layers.'),
    (2, '2026-01-01 09:00:00', 'Ethan Zhao', 'ethan@academy.test', 'Retrieval Systems', 'Builds search and recommendation systems on relational storage.'),
    (3, '2026-01-01 09:00:00', 'Maya Chen', 'maya@academy.test', 'Reliability Engineering', 'Leads platform reliability programs and incident learning workshops.'),
    (4, '2026-01-01 09:00:00', 'Priya Singh', 'priya@academy.test', 'API Design', 'Teaches API modeling, versioning, and developer experience.');

INSERT INTO "learner" ("id", "created_at", "name", "email", "company") VALUES
    (1, '2026-01-01 09:00:00', 'Alice Kim', 'alice@acme.test', 'Acme Cloud'),
    (2, '2026-01-01 09:00:00', 'Bruno Diaz', 'bruno@acme.test', 'Acme Cloud'),
    (3, '2026-01-01 09:00:00', 'Chloe Rao', 'chloe@river.test', 'Blue River Data'),
    (4, '2026-01-01 09:00:00', 'Diego Park', 'diego@northwind.test', 'Northwind Systems'),
    (5, '2026-01-01 09:00:00', 'Eva Lin', 'eva@river.test', 'Blue River Data');

INSERT INTO "course" (
    "id",
    "created_at",
    "track_id",
    "instructor_id",
    "prerequisite_id",
    "title",
    "summary",
    "level",
    "list_price_cents",
    "published"
) VALUES
    (1, '2026-01-02 09:00:00', 1, 1, 0, 'Go Services with SQLite', 'Use SQLite-backed repositories and queries inside production-grade Go services.', 0, 120000, 1),
    (2, '2026-01-03 09:00:00', 1, 4, 1, 'API Design Workshop', 'Design stable HTTP and internal APIs for service-oriented backend teams.', 1, 150000, 1),
    (3, '2026-01-04 09:00:00', 1, 1, 2, 'Event-Driven Backend Systems', 'Coordinate asynchronous workflows, queues, and durable backend contracts.', 2, 165000, 1),
    (4, '2026-01-05 09:00:00', 2, 2, 0, 'Retrieval Systems with SQLite', 'Prototype retrieval pipelines, ranking loops, and relevance signals on SQLite.', 1, 180000, 1),
    (5, '2026-01-06 09:00:00', 2, 2, 4, 'Applied Embedding Pipelines', 'Move from retrieval prototypes to embedding-backed production workflows.', 2, 210000, 1),
    (6, '2026-01-07 09:00:00', 3, 3, 0, 'Incident Automation Fundamentals', 'Build automation that shortens incident response and routine operations.', 0, 90000, 1),
    (7, '2026-01-08 09:00:00', 3, 3, 6, 'SLO Engineering for On-Call Teams', 'Turn reliability targets into dashboards, alerts, and operational reviews.', 1, 130000, 1);

INSERT INTO "enrollment" (
    "uid",
    "created_at",
    "updated_at",
    "deleted_at",
    "version",
    "learner_id",
    "course_id",
    "status",
    "score",
    "fee_cents"
) VALUES
    (1, '2026-02-01 10:00:00', NULL, 0, 1, 1, 1, 1, 95, 120000),
    (2, '2026-02-01 10:05:00', NULL, 0, 1, 2, 1, 0, 84, 120000),
    (3, '2026-02-02 11:00:00', NULL, 0, 1, 3, 2, 0, 88, 150000),
    (4, '2026-02-02 11:10:00', NULL, 0, 1, 1, 2, 1, 92, 150000),
    (5, '2026-02-03 09:30:00', NULL, 0, 1, 5, 3, 0, 86, 165000),
    (6, '2026-02-03 09:40:00', NULL, 0, 1, 3, 3, 1, 90, 165000),
    (7, '2026-02-04 13:00:00', NULL, 0, 1, 3, 4, 0, 91, 180000),
    (8, '2026-02-04 13:20:00', NULL, 0, 1, 5, 4, 1, 97, 180000),
    (9, '2026-02-05 08:30:00', NULL, 0, 1, 5, 5, 0, 89, 210000),
    (10, '2026-02-05 08:35:00', NULL, 0, 1, 4, 5, 2, 0, 210000),
    (11, '2026-02-06 14:00:00', NULL, 0, 1, 2, 6, 0, 78, 90000),
    (12, '2026-02-06 14:10:00', NULL, 0, 1, 1, 6, 3, 0, 90000),
    (13, '2026-02-07 15:00:00', NULL, 0, 1, 1, 7, 0, 82, 130000);
