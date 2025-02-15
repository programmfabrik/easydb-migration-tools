CREATE TABLE IF NOT EXISTS "item" (
    "file"      TEXT NOT NULL,
    "item_id"   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS "value" (
    "item_id"   TEXT NOT NULL,
    "feld"      TEXT NOT NULL,
    "unterfeld" TEXT NOT NULL,
    "idx"       INTEGER NOT NULL,
    "wert"      TEXT NOT NULL
);
