#!/bin/bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <архів_зашифрованих_частин> <приватний_ключ>"
  exit 1
fi

ARCHIVE_FILE=$1
PRIVATE_KEY=$2
DECRYPTED_DIR="decrypted_parts"
OUTPUT_FILE="restored_file"

# Створюємо папку для дешифрованих частин
mkdir -p "$DECRYPTED_DIR"

# Крок 1: Розпаковуємо архів
echo "Розпаковуємо архів $ARCHIVE_FILE..."
tar -xzf "$ARCHIVE_FILE" -C "$DECRYPTED_DIR"

# Крок 2: Дешифруємо кожну частину
echo "Дешифруємо частини..."
for enc_part in "$DECRYPTED_DIR"/part_*.enc; do
  OUTPUT_PART="${enc_part%.enc}"
  openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$OUTPUT_PART"
  rm "$enc_part" # Видаляємо зашифровану частину
done

# Крок 3: Об'єднуємо частини у вихідний файл
echo "Об'єднуємо частини у файл $OUTPUT_FILE..."
cat "$DECRYPTED_DIR"/part_* > "$OUTPUT_FILE"

# Очищаємо тимчасову папку
rm -rf "$DECRYPTED_DIR"

echo "Файл відновлено: $OUTPUT_FILE"
