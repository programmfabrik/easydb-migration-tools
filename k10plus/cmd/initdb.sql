CREATE TABLE IF NOT EXISTS "file" (
    "id"        INTEGER PRIMARY KEY AUTOINCREMENT,
    "filepath"  TEXT NOT NULL,
    UNIQUE("filepath")
);
CREATE TABLE IF NOT EXISTS "item" (
    "id"        INTEGER PRIMARY KEY AUTOINCREMENT,
    "file_id"   INTEGER NOT NULL,
    FOREIGN KEY("file_id") REFERENCES "file"("id")
);
CREATE TABLE IF NOT EXISTS "value" (
    "id"        INTEGER PRIMARY KEY AUTOINCREMENT,
    "item_id"   INTEGER NOT NULL,
    "feld"      TEXT NOT NULL,
    "unterfeld" TEXT NOT NULL,
    "wert"      TEXT NOT NULL,
    FOREIGN KEY("item_id") REFERENCES "item"("id")
);
