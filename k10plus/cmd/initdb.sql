CREATE TABLE IF NOT EXISTS "k10_file" (
    --SERIAL--
    "filepath"  TEXT NOT NULL,
    UNIQUE("filepath")
);
CREATE TABLE IF NOT EXISTS "k10_item" (
    --SERIAL--
    "file_id"   INTEGER NOT NULL,
    FOREIGN KEY("file_id") REFERENCES "k10_file"("id")
);
CREATE TABLE IF NOT EXISTS "k10_value" (
    --SERIAL--
    "item_id"   INTEGER NOT NULL,
    "feld"      TEXT NOT NULL,
    "unterfeld" TEXT NOT NULL,
    "wert"      TEXT NOT NULL
    -- FOREIGN KEY("item_id") REFERENCES "k10_item"("id")
);
CREATE INDEX IF NOT EXISTS "idx_k10_item_file_id" ON "k10_item"("file_id");
CREATE INDEX IF NOT EXISTS "idx_k10_value_item_id" ON "k10_value"("item_id");
